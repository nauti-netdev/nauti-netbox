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
from operator import itemgetter
import asyncio

from nauti.collection import Collection, CollectionCallback
from nauti.collections.ipaddrs import IPAddrCollection
from nauti_netbox.source import NetboxSource, NetboxClient

_IPAM_ADDR_URL = "/ipam/ip-addresses/"


class NetboxIPAddrCollection(Collection, IPAddrCollection):

    source_class = NetboxSource

    async def fetch(self, **filters):
        """ fetch args are Netbox specific API parameters """

        self.source_records.extend(
            await self.source.client.paginate(url=_IPAM_ADDR_URL, filters=filters)
        )

    async def fetch_items(self, items):
        await asyncio.gather(
            *(
                self.fetch(hostname=rec["hostname"], address=rec["ipaddr"])
                for rec in items.values()
            )
        )

    def itemize(self, rec: Dict) -> Dict:
        # if IP address is not assgined to an interface, then leave those field
        # values as empty-string

        # TODO: need to handle difference between the v2.8 release and v2.9 release
        #       where Netbox changed the API body format.

        if not (if_dat := rec.get("assigned_object")):
            return {"ipaddr": rec["address"], "hostname": "", "interface": ""}

        return {
            "ipaddr": rec["address"],
            "hostname": if_dat["device"]["name"],
            "interface": if_dat["name"],
        }

    async def add_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):

        client: NetboxClient = self.source.client

        # for each missing record we will need to fetch the interface record so
        # we can bind the address to it.

        if_key_fn = itemgetter("hostname", "interface")
        if_items = map(if_key_fn, items.values())
        if_recs = await client.fetch_devices_interfaces(if_items)
        if_lkup = {(rec["device"]["name"], rec["name"]): rec for rec in if_recs}

        api = self.source.client

        def _create_task(key, fields):
            if_key = if_key_fn(fields)
            if (if_rec := if_lkup.get(if_key)) is None:
                print(
                    "SKIP: ipaddr {}, missing interface: {}, {}.".format(key, *if_key)
                )
                return None

            payload = dict(
                address=fields["ipaddr"],
                assigned_object_type="dcim.interface",
                assigned_object_id=if_rec["id"],
            )

            if if_rec["name"].lower().startswith("loopback"):
                payload["role"] = "loopback"

            return api.post(url=_IPAM_ADDR_URL, json=payload)

        await self.source.update(items, callback, _create_task)

    async def update_items(
        self, changes: Dict, callback: Optional[CollectionCallback] = None
    ):
        # for each update record we will need to fetch the interface record so
        # we can bind the address to it.

        client: NetboxClient = self.source.client

        # at this time the only change allowed is the re-assignment of IP
        # address to interface. therefore if there is any chance record that
        # does *not* include an interface field then we raise an exception.

        try:
            if_items = [
                (ch_key[0], ch_val["interface"]) for ch_key, ch_val in changes.items()
            ]
        except Exception:
            raise RuntimeError(
                f"{self.source.name}:{self.name} - only IP address interface re-assignment supported"
            )

        if_recs = await client.fetch_devices_interfaces(if_items)
        if_lkup = {(rec["device"]["name"], rec["name"]): rec for rec in if_recs}

        def _update_task(key, fields):
            hostname, if_ipaddr = key
            if_rec = if_lkup[(hostname, fields["interface"])]
            payload = dict(
                address=if_ipaddr,
                assigned_object_type="dcim.interface",
                assigned_object_id=if_rec["id"],
            )

            orig_ipam_rec_id = self.source_record_keys[key]["id"]
            return client.patch(
                url=_IPAM_ADDR_URL + f"{orig_ipam_rec_id}/", json=payload
            )

        await self.source.update(changes, callback, _update_task)

    async def delete_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        client: NetboxClient = self.source.client

        def _delete_task(key, fields):
            orig_ipam_rec_id = self.source_record_keys[key]["id"]
            return client.delete(url=_IPAM_ADDR_URL + f"{orig_ipam_rec_id}/")

        await self.source.update(items, callback, _delete_task)
