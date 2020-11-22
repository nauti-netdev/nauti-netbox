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
# Private Imports
# -----------------------------------------------------------------------------

from nauti.collection import Collection, CollectionCallback
from nauti.collections.interfaces import InterfaceCollection
from nauti_netbox.source import NetboxSource, NetboxClient

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------

__all__ = ["NetboxInterfaceCollection"]


# -----------------------------------------------------------------------------
#
#                              CODE BEGINS
#
# -----------------------------------------------------------------------------


class NetboxInterfaceCollection(Collection, InterfaceCollection):
    source_class = NetboxSource

    async def fetch(self, hostname, **params):
        """
        fetch interfaces must be done on a per-device (hostname) basis.
        fetch args are Netbox API specific.
        """

        self.source_records.extend(
            await self.source.client.paginate(
                url="/dcim/interfaces/", filters=dict(device=hostname, **params)
            )
        )

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
            description=rec["description"],
        )

    async def create_items(
        self, missing, callback: Optional[CollectionCallback] = None
    ):
        client: NetboxClient = self.source.client

        device_records = await client.fetch_devices(
            hostname_list=(rec["hostname"] for rec in missing.values()), key="name"
        )

        def _create_task(key, fields):
            hostname, if_name = key
            if hostname not in device_records:
                print(f"ERROR: device {hostname} missing.")
                return None

            # TODO: set the interface type correctly based on some kind of mapping definition.
            #       for now, use this name-basis for loopback, vlan, port-channel.

            if_type = {
                "v": "virtual",  # vlan
                "l": "virtual",  # loopback
                "p": "lag",  # port-channel
            }.get(if_name[0].lower(), "other")

            return client.post(
                url="/dcim/interfaces/",
                json=dict(
                    device=device_records[hostname]["id"],
                    name=if_name,
                    description=fields["description"],
                    type=if_type,
                ),
            )

        await self.source.update(
            updates=missing, callback=callback, creator=_create_task
        )

    async def update_items(
        self, changes: Dict, callback: Optional[CollectionCallback] = None
    ):
        # Presently the only field to update is description; so we don't need to put
        # much logic into this post body process.  Might need to in the future.

        client = self.source.client

        def _create_task(key, fields):
            if_id = self.source_record_keys[key]["id"]
            return client.patch(
                url=f"/dcim/interfaces/{if_id}/",
                json=dict(description=fields["description"]),
            )

        await self.source.update(changes, callback, _create_task)
