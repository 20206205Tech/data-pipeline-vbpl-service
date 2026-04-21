from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user, require_admin
from app.documents import service
from common.response.base_response import BaseResponse
from database.config import get_db
from utils.log_function import log_function

router = APIRouter(prefix="/documents", tags=["documents"])


@log_function
@router.get("/total", response_model=BaseResponse[Any])
def get_document_total(db: Session = Depends(get_db), user=Depends(get_current_user)):
    """Lấy thông tin tổng số văn bản"""
    data = service.get_document_total(db)
    return BaseResponse(
        success=True, message="Lấy tổng số văn bản thành công", data=data
    )


@log_function
@router.get(
    "/status", dependencies=[Depends(require_admin)], response_model=BaseResponse[Any]
)
def get_document_status_report(db: Session = Depends(get_db)):
    """Báo cáo trạng thái văn bản"""
    data = service.get_document_status_report(db)
    return BaseResponse(
        success=True, message="Lấy báo cáo trạng thái thành công", data=data
    )


@log_function
@router.get(
    "/recent", dependencies=[Depends(require_admin)], response_model=BaseResponse[Any]
)
def get_recent_documents(limit: int = 10, db: Session = Depends(get_db)):
    """Lấy danh sách gần đây"""
    data = service.get_recent_documents(db, limit=limit)
    return BaseResponse(
        success=True, message="Lấy danh sách gần đây thành công", data=data
    )


@log_function
@router.get(
    "/issue-date",
    dependencies=[Depends(require_admin)],
    response_model=BaseResponse[Any],
)
def get_issue_date_report(db: Session = Depends(get_db)):
    """Thống kê theo năm"""
    data = service.get_issue_date_report(db)
    return BaseResponse(success=True, message="Thống kê theo năm thành công", data=data)


# @router.get(
#     "/info", dependencies=[Depends(require_admin)], response_model=BaseResponse[Any]
# )
# @log_function
# def get_document_info(
#     item_id: Optional[int] = None,
#     document_number: Optional[str] = None,
#     db: Session = Depends(get_db),
# ):
#     """Lấy thông tin chi tiết của văn bản theo item_id hoặc document_number"""
#     data = service.get_document_info_detail(
#         db, item_id=item_id, document_number=document_number
#     )
#     return BaseResponse(
#         success=True, message="Lấy thông tin văn bản thành công", data=data
#     )
