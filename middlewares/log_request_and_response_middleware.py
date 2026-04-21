import json
import time

from fastapi import Request
from starlette.background import BackgroundTask
from starlette.middleware.base import BaseHTTPMiddleware

from database.config import SessionLocal
from database.models import RequestLog

# Giới hạn kích thước payload tối đa để ghi log (Ví dụ: 5MB)
MAX_PAYLOAD_SIZE = 5 * 1024 * 1024


def write_log_to_db(log_data: dict):
    """Hàm ghi log vào database chạy dưới dạng Background Task"""
    db = SessionLocal()
    try:
        new_log = RequestLog(**log_data)
        db.add(new_log)
        db.commit()
    except Exception as e:
        print(f"Lỗi khi ghi log request: {e}")
    finally:
        db.close()


async def get_request_body(request: Request) -> bytes:
    """Hàm hỗ trợ đọc và khôi phục Request Body"""
    body = await request.body()

    async def receive():
        return {"type": "http.request", "body": body}

    request._receive = receive
    return body


class LogRequestAndResponseMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # 1. Lấy correlation-id từ Kong API Gateway (nếu có)
        request_id = request.headers.get("request-id")

        # ==========================================
        # 1. KIỂM TRA VÀ ĐỌC REQUEST BODY
        # ==========================================
        req_content_type = request.headers.get("content-type", "")
        req_content_length = int(request.headers.get("content-length", 0))

        is_multipart = "multipart/form-data" in req_content_type
        is_large_req = req_content_length > MAX_PAYLOAD_SIZE

        req_body_json = None
        # Chỉ đọc request body nếu KHÔNG PHẢI upload file VÀ dung lượng NHỎ HƠN mức cho phép
        if not is_multipart and not is_large_req:
            req_body_bytes = await get_request_body(request)
            if req_body_bytes:
                try:
                    req_body_json = json.loads(req_body_bytes.decode("utf-8"))
                except Exception:
                    pass
        else:
            req_body_json = {
                "note": f"Skipped logging. Multipart: {is_multipart}, Size: {req_content_length} bytes"
            }

        # Chuyển request đi tiếp tới Endpoint
        response = await call_next(request)

        # ==========================================
        # 2. KIỂM TRA VÀ ĐỌC RESPONSE BODY
        # ==========================================
        res_content_type = response.headers.get("content-type", "")
        res_content_length_str = response.headers.get("content-length")
        # Header content-length có thể không tồn tại nếu FastAPI dùng chunked transfer
        res_content_length = (
            int(res_content_length_str)
            if res_content_length_str and res_content_length_str.isdigit()
            else 0
        )

        # An toàn nhất là chỉ cố gắng đọc response nếu server trả về JSON
        is_json_response = "application/json" in res_content_type
        is_large_res = res_content_length > MAX_PAYLOAD_SIZE

        res_body_json = None

        if is_json_response and not is_large_res:
            res_body_bytes = b""
            if hasattr(response, "body_iterator"):
                res_body_chunks = [chunk async for chunk in response.body_iterator]
                res_body_bytes = b"".join(res_body_chunks)

                async def new_body_iterator():
                    for chunk in res_body_chunks:
                        yield chunk

                response.body_iterator = new_body_iterator()

            if res_body_bytes:
                try:
                    res_body_json = json.loads(res_body_bytes.decode("utf-8"))
                except Exception:
                    pass
        else:
            res_body_json = {
                "note": f"Skipped logging. Content-Type: {res_content_type}, Size: {res_content_length} bytes"
            }

        process_time = time.time() - start_time

        # ==========================================
        # 3. GOM DỮ LIỆU VÀ GHI DATABASE
        # ==========================================
        log_data = {
            "request_id": request_id,
            "method": request.method,
            "url": str(request.url.path),
            "client_ip": request.client.host if request.client else None,
            "status_code": response.status_code,
            "request_payload": req_body_json,
            "response_payload": res_body_json,
            "process_time": process_time,
        }

        if response.background:
            response.background.add_task(write_log_to_db, log_data)
        else:
            response.background = BackgroundTask(write_log_to_db, log_data)

        return response
