"""
Agent 환경변수 설정
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class AgentSettings(BaseSettings):
    """Agent 설정"""

    # 거래소 설정
    exchange: str = "bybit"
    api_key: str
    api_secret: str
    api_passphrase: str = ""      # Bitget/OKX 전용

    # 추천인 코드 (마케터의 Bitget UID — 에이전트 등록 시 필수)
    referral_code: str = ""

    # Agent 인증 (형식: {supabase_user_id}:{32바이트_hex})
    agent_token: str              # 앱에서 발급받은 토큰

    # 중앙 서버 연결
    central_url: str              # e.g. https://central-server.railway.app

    # 서버 포트 (Railway 자동 제공)
    port: int = 8000

    @property
    def user_id(self) -> str:
        """토큰에서 user_id 추출 ({user_id}:{token_secret} 형식)"""
        return self.agent_token.split(":")[0]

    @property
    def token_secret(self) -> str:
        """토큰에서 HMAC 서명 키 추출"""
        return self.agent_token.split(":", 1)[1]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


settings = AgentSettings()
