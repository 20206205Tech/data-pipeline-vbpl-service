from typing import Generic, Optional, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class BaseResponse(BaseModel, Generic[T]):
    success: bool = Field(..., description="Success")
    message: str = Field(..., description="Message")
    data: Optional[T] = Field(default=None, description="Data collection or object")
