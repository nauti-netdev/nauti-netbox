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

from typing import Dict, Optional
import asyncio

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

from nauti.igather import igather

# -----------------------------------------------------------------------------
# Private Imports
# -----------------------------------------------------------------------------

from nauti.collection import Collection, CollectionCallback, get_collection
from nauti.collections.portchans import PortChannelCollection
from nauti_netbox.source import NetboxSource, NetboxClient

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------

__all__ = ["NetboxPortChanCollection"]


# -----------------------------------------------------------------------------
#
#                              CODE BEGINS
#
# -----------------------------------------------------------------------------

_INTFS_URL = "/dcim/interfaces/"
_LAG_KEY_ = "__lag__"


class NetboxPortChanCollection(Collection, PortChannelCollection):
    source_class = NetboxSource

    async def fetch(self, hostname, **params):
        """
        fetch interfaces must be done on a per-device (hostname) basis.
        fetch args are Netbox API specific.
        """

        # if (col_ifaces := self.cache.get('interfaces')) is None:
        #     col_ifaces = self.cache['interfaces'] = get_collection(
        #         source=self, name='interfaces'
        #     )

        nb_api: NetboxClient = self.source.client

        nb_filters = params.copy()
        nb_filters["device"] = hostname
        nb_filters["type"] = "lag"

        lag_records = await nb_api.paginate(url=_INTFS_URL, filters=nb_filters)

        # create a cache of the known LAG interfaces because we will need these
        # later in the create/update methods.

        self.cache[self] = dict()

        self.cache[self]["lag_recs"] = {
            (hostname, lag_rec["name"]): lag_rec for lag_rec in lag_records
        }

        for lag_rec in lag_records:
            res = await nb_api.get(_INTFS_URL, params={"lag_id": lag_rec["id"]})
            for if_rec in res.json()["results"]:
                if_rec[_LAG_KEY_] = lag_rec
                self.source_records.append(if_rec)

    async def fetch_keys(self, keys: Dict):
        await asyncio.gather(
            *(
                self.fetch(hostname=rec["hostname"], name=rec["interface"])
                for rec in keys.values()
            )
        )

    def itemize(self, rec: Dict) -> Dict:
        return dict(
            hostname=rec["device"]["name"],
            interface=rec["name"],
            portchan=rec[_LAG_KEY_]["name"],
        )

    async def create_items(
        self, missing: Dict, callback: Optional[CollectionCallback] = None
    ):
        # missing items means that the existing interface does not have any
        # associated LAG.  We need to patch the interface record with the
        # LAG id.
        api: NetboxClient = self.source.client

        # we first need to retrieve all of the interface records
        col_ifaces = get_collection(source=self.source, name="interfaces")

        async for _ in igather(
            (
                col_ifaces.fetch(hostname=item["hostname"], name=item["interface"])
                for item in missing.values()
            ),
            limit=100,
        ):
            pass

        col_ifaces.make_keys()

        def _patch(key, item):
            if_rec = col_ifaces.source_record_keys[key]
            lag_key = (item["hostname"], item["portchan"])
            lag_rec = self.cache[self]["lag_recs"][lag_key]
            return api.patch(
                _INTFS_URL + f"{if_rec['id']}/", json=dict(lag=lag_rec["id"])
            )

        await self.source.update(missing, callback=callback, creator=_patch)

    async def update_items(
        self, changes: Dict, callback: Optional[CollectionCallback] = None
    ):
        # we first need to retrieve all of the interface records
        col_ifaces = get_collection(source=self.source, name="interfaces")

        async for _ in igather(
            (
                col_ifaces.fetch(
                    hostname=item.fingerprint["hostname"],
                    name=item.fingerprint["interface"],
                )
                for item in changes.values()
            ),
            limit=100,
        ):
            pass

        col_ifaces.make_keys()

        api: NetboxClient = self.source.client

        def _patch(_key, _ch_fields):
            if_rec = col_ifaces.source_record_keys[_key]
            col_fields = self.inventory[_key]
            lag_key = (col_fields["hostname"], _ch_fields["portchan"])
            lag_rec = self.cache[self]["lag_recs"][lag_key]
            return api.patch(
                _INTFS_URL + f"{if_rec['id']}/", json=dict(lag=lag_rec["id"])
            )

        await self.source.update(changes, callback=callback, creator=_patch)

    async def remove_extra(
        self, extras: Dict, callback: Optional[CollectionCallback] = None
    ):
        api: NetboxClient = self.source.client

        # we first need to retrieve all of the interface records
        col_ifaces = get_collection(source=self.source, name="interfaces")

        async for _ in igather(
            (
                col_ifaces.fetch(hostname=item["hostname"], name=item["interface"])
                for item in extras.values()
            ),
            limit=100,
        ):
            pass

        col_ifaces.make_keys()

        def _patch(key, _fields):
            if_rec = col_ifaces.source_record_keys[key]
            return api.patch(_INTFS_URL + f"{if_rec['id']}/", json=dict(lag=None))

        await self.source.update(extras, callback=callback, creator=_patch)
