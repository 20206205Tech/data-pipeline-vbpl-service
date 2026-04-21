from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .enums import UserRole
from .schemas import CurrentUser
from .security import verify_token

security = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> CurrentUser:
    token = credentials.credentials
    payload = verify_token(token)

    app_metadata = payload.get("app_metadata", {})
    role = app_metadata.get("role") or payload.get("role")

    return CurrentUser(
        user_id=payload.get("sub"), email=payload.get("email"), role=role
    )


def get_current_user_id(user: CurrentUser = Depends(get_current_user)) -> str:
    return user.user_id


def require_admin(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    if user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden - Requires Admin role",
        )
    return user
