#      Copyright (C) 2020  Jeremy Schulman
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.

# -----------------------------------------------------------------------------
# System Imports
# -----------------------------------------------------------------------------

from typing import Optional

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

from nauti.source import Source
from nauti.config_models import SourcesModel

# -----------------------------------------------------------------------------
# Private Imports
# -----------------------------------------------------------------------------

from nauti_netbox import NAUTI_SOURCE_NAME
from nauti_netbox.client import NetboxClient


__all__ = ["NetboxSource", "NetboxClient"]


class NetboxSource(Source):

    name = NAUTI_SOURCE_NAME
    client_class = NetboxClient

    def __init__(self, config: Optional[SourcesModel] = None, **kwargs):
        super(NetboxSource, self).__init__()
        initargs = dict()

        if config:
            initargs.update(
                dict(
                    base_url=config.default.url,
                    token=config.default.credentials.token.get_secret_value(),
                    **config.default.options,
                )
            )
            initargs.update(kwargs)

        self._client_initargs = initargs or kwargs

        # the client instance will be created in the `login` method.
        self.client = None

    async def login(self, *vargs, **kwargs):
        # when executing the login, create the new REST client.
        self.client = NetboxClient(**self._client_initargs)

    async def logout(self):
        await self.client.aclose()

    @property
    def is_connected(self):
        return False if not self.client else (not self.client.is_closed)
