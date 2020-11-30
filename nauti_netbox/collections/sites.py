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

from typing import Dict, Optional

from nauti.slugify import slugify
from nauti.collection import Collection, CollectionCallback
from nauti.collections.sites import SiteCollection
from nauti_netbox.source import NetboxSource, NetboxClient


class NetboxSiteCollection(Collection, SiteCollection):
    source_class = NetboxSource

    async def fetch(self, **filters):
        self.source_records.extend(await self.source.client.paginate(url="/dcim/sites"))

    async def fetch_items(self, items: Dict):
        pass

    def itemize(self, rec: Dict) -> Dict:
        return {"name": rec["slug"]}

    async def add_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        api: NetboxClient = self.source.client

        def _creator(key, item):  # noqa
            name = key
            return api.post(
                url="/dcim/sites/", json={"name": name, "slug": slugify(name)}
            )

        await self.source.update(updates=items, callback=callback, creator=_creator)

    async def update_items(
        self, changes: Dict, callback: Optional[CollectionCallback] = None
    ):
        raise NotImplementedError()

    async def delete_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        raise NotImplementedError()
