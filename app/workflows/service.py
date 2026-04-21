from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from utils.log_function import log_function


@log_function
def get_all_workflows(db: Session):
    query = text("SELECT id, code, description FROM workflows")
    result = db.execute(query).fetchall()
    return [{"id": r[0], "code": r[1], "description": r[2]} for r in result]


@log_function
def get_pipeline_summary(db: Session):
    """
    Thống kê số lượng văn bản đã hoàn thành theo từng loại workflow.
    """
    query = text(
        """
        SELECT
            w.id as workflow_id,
            w.code,
            COUNT(ds.item_id) as total_items
        FROM document_state ds
        JOIN workflows w ON ds.workflow_id = w.id
        WHERE ds.end_time IS NOT NULL
        GROUP BY w.id, w.code
        ORDER BY w.id ASC
        """
    )
    try:
        result = db.execute(query).fetchall()
        return [
            {"workflow_id": row[0], "code": row[1], "total_items": row[2]}
            for row in result
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
