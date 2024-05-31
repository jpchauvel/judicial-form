import asyncio
import random
from typing import Self

from evpn import ExpressVpnApi


class AsyncExpressVpnApi:
    def __init__(self) -> None:
        self.api: ExpressVpnApi = ExpressVpnApi()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await asyncio.to_thread(self.api.disconnect)
        await asyncio.to_thread(self.api.close)

    async def rotate_vpn(self) -> None:
        passed: bool = False
        while not passed:
            try:
                await asyncio.to_thread(
                    self.api.connect, get_random_location(self.api)
                )
                await asyncio.sleep(5)
            except Exception:
                self.api: ExpressVpnApi = ExpressVpnApi()
            else:
                passed = True


def get_random_location(api: ExpressVpnApi) -> str:
    return random.choice(api.locations)["id"]
