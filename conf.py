from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    anti_captcha_api_key_path: str = ""
    url: str = ""
    since: int = 0
    timeout: int = 0
    delay_after_click: int = 0
    threshold: float = 0.0
    headless: bool = True

    model_config = SettingsConfigDict(env_file=".env")

    def __init__(self, **data) -> None:
        super().__init__(**data)
        self._anti_captcha_api_key = (
            Path(self.anti_captcha_api_key_path)
            .expanduser()
            .read_text()
            .strip()
        )

    @property
    def anti_captcha_api_key(self) -> str:
        return self._anti_captcha_api_key


@lru_cache
def get_settings() -> Settings:
    return Settings()
