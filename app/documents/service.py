from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from utils.log_function import log_function


@log_function
def get_document_total(db: Session):
    """Lấy thông tin tổng số văn bản từ bản ghi mới nhất"""
    query = text(
        """
        SELECT total_count, update_at
        FROM document_total
        ORDER BY update_at DESC
        LIMIT 1
        """
    )
    try:
        row = db.execute(query).fetchone()
        if not row:
            return {"total_count": 0, "update_at": None}
        return {"total_count": row[0], "update_at": row[1]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi document_total: {str(e)}")


@log_function
def get_document_status_report(db: Session):
    """Báo cáo số lượng văn bản phân loại theo trạng thái"""
    query = text(
        """
        SELECT
            status,
            COUNT(item_id) as total_count,
            MIN(update_at) as oldest_update,
            MAX(update_at) as latest_update
        FROM document_info
        GROUP BY status
        ORDER BY total_count DESC
        """
    )
    try:
        result = db.execute(query).fetchall()
        return [
            {
                "status": row[0],
                "count": row[1],
                "oldest_update": row[2],
                "latest_update": row[3],
            }
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi document_info: {str(e)}")


@log_function
def get_recent_documents(db: Session, limit: int = 10):
    query = text(
        """
        SELECT ds.item_id, w.code, ds.end_time
        FROM document_state ds
        JOIN workflows w ON ds.workflow_id = w.id
        WHERE ds.end_time IS NOT NULL
        ORDER BY ds.end_time DESC
        LIMIT :limit
    """
    )
    try:
        result = db.execute(query, {"limit": limit}).fetchall()
        return [
            {"item_id": row[0], "step_code": row[1], "completed_at": row[2]}
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi recent_documents: {str(e)}")


@log_function
def get_issue_date_report(db: Session):
    """Thống kê số lượng văn bản theo năm ban hành"""
    query = text(
        """
        SELECT
            EXTRACT(YEAR FROM issue_date)::INTEGER as issue_year,
            COUNT(item_id) as total_count
        FROM document_info
        WHERE issue_date IS NOT NULL
        GROUP BY issue_year
        ORDER BY issue_year DESC
    """
    )
    try:
        result = db.execute(query).fetchall()
        return [{"year": row[0], "count": row[1]} for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi issue_date_report: {str(e)}")


# def get_document_info_detail(
#     db: Session, item_id: Optional[int] = None, document_number: Optional[str] = None
# ):
#     """Lấy thông tin chi tiết của văn bản theo item_id hoặc document_number"""
#     if not item_id and not document_number:
#         raise HTTPException(
#             status_code=400, detail="Vui lòng cung cấp item_id hoặc document_number"
#         )

#     conditions = []
#     params = {}

#     if item_id:
#         conditions.append("item_id = :item_id")
#         params["item_id"] = item_id
#     if document_number:
#         conditions.append("document_number = :document_number")
#         params["document_number"] = document_number

#     where_clause = " OR ".join(conditions)

#     query = text(
#         f"""
#         SELECT
#             item_id, status, effective_date, issuing_agency,
#             document_number, issue_date, title, signer, position, update_at
#         FROM document_info
#         WHERE {where_clause}
#         LIMIT 1
#     """
#     )

#     try:
#         row = db.execute(query, params).fetchone()
#         if not row:
#             raise HTTPException(
#                 status_code=404, detail="Không tìm thấy thông tin văn bản"
#             )

#         return {
#             "item_id": row[0],
#             "status": row[1],
#             "effective_date": row[2],
#             "issuing_agency": row[3],
#             "document_number": row[4],
#             "issue_date": row[5],
#             "title": row[6],
#             "signer": row[7],
#             "position": row[8],
#             "update_at": row[9],
#         }
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(
#             status_code=500, detail=f"Lỗi khi lấy thông tin văn bản: {str(e)}"
#         )
