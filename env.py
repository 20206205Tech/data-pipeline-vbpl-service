from environs import Env
from loguru import logger

env = Env()
logger.info("Loading environment variables...")


DATA_PIPELINE_VBPL_DATABASE_URL = env.str("DATA_PIPELINE_VBPL_DATABASE_URL")
DATABASE_URL = DATA_PIPELINE_VBPL_DATABASE_URL
SERVICE_NAME = "data-pipeline-service"
PORT = env.int("SERVICE_PORT", 30000)


ENVIRONMENT = env.str("ENVIRONMENT", "production")
RELOAD = True if ENVIRONMENT == "development" else False


SUPABASE_PROJECT_ID = env.str("SUPABASE_PROJECT_ID")
SUPABASE_URL = f"https://{SUPABASE_PROJECT_ID}.supabase.co"
JWKS_URL = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"
ISSUER = f"{SUPABASE_URL}/auth/v1"
AUDIENCE = "authenticated"


DESCRIPTION = f"""
# Chào mừng đến với {SERVICE_NAME} ({ENVIRONMENT})

* [Local API](http://localhost:{PORT})
* [Local Swagger](http://localhost:{PORT}/docs)
* [Dev API](https://dev-code-{SERVICE_NAME}.20206205.tech)
* [Dev Swagger](https://dev-code-{SERVICE_NAME}.20206205.tech/docs)
* [Đăng nhập với Google](https://{SUPABASE_PROJECT_ID}.supabase.co/auth/v1/authorize?provider=google)

""".strip()

logger.success(f"DESCRIPTION: \n{DESCRIPTION}")
