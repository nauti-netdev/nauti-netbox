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
from nauti.collections.devices import DeviceCollection
from nauti_netbox.source import NetboxSource, NetboxClient
from nauti.mappings import normalize_hostname

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------

__all__ = ["NetboxDeviceCollection"]


# -----------------------------------------------------------------------------
#
#                              CODE BEGINS
#
# -----------------------------------------------------------------------------

_DEVICES_URL = "/dcim/devices/"


class NetboxDeviceCollection(Collection, DeviceCollection):

    source_class = NetboxSource

    async def fetch(self, filters=None):
        _filters = dict(exclude="config_context", name__n="null")
        if filters:
            _filters.update(filters)

        self.source_records.extend(
            await self.source.client.paginate(url=_DEVICES_URL, filters=_filters)
        )

    def itemize(self, rec: Dict) -> Dict:
        dt = rec["device_type"]

        try:
            ipaddr = rec["primary_ip"]["address"].split("/")[0]
        except (TypeError, KeyError):
            ipaddr = ""

        try:
            os_name = rec["platform"]["slug"]
        except (TypeError, KeyError):
            os_name = ""

        return dict(
            sn=rec["serial"],
            hostname=normalize_hostname(rec["name"]),
            ipaddr=ipaddr,
            site=rec["site"]["slug"],
            os_name=os_name,
            vendor=dt["manufacturer"]["slug"],
            model=dt["slug"],
            status=rec["status"]["value"],
        )

    async def add_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        nb_api = self.source.client

        device_types, sites, device_role, platforms = await asyncio.gather(
            nb_api.paginate(url="/dcim/device-types/"),
            nb_api.paginate(url="/dcim/sites/"),
            nb_api.paginate(url="/dcim/device-roles/", filters={"slug": "unassigned"}),
            nb_api.paginate(url="/dcim/platforms/"),
        )

        device_types = {rec["slug"]: rec["id"] for rec in device_types}
        sites = {rec["slug"]: rec["id"] for rec in sites}
        role_unknwon = device_role[0]["id"]
        platforms = {rec["slug"]: rec["id"] for rec in platforms}

        def _create_task(key, fields):  # noqa
            model = fields["model"]
            hostname = fields["hostname"]
            dt_slug = self.imap_field_value("model", model)

            # if (dt_slug := config.maps["models"].get(model, "")) == "":
            #     print(
            #         f"ERROR: {hostname}, no device-type mapping for model {model}, skipping."
            #     )
            #     return None

            if (dt_id := device_types.get(dt_slug)) is None:
                print(
                    f"ERROR: {hostname}, no device-type for slug {dt_slug}, skipping."
                )
                return None

            if (site_id := sites.get(fields["site"])) is None:
                print(f"ERROR: {hostname}, missing site {fields['site']}, skipping.")
                return None

            if (pl_id := platforms.get(fields["os_name"])) is None:
                print(
                    f"ERROR: {hostname}, missing platform {fields['os_name']}, skipping."
                )
                return None

            return nb_api.post(
                url="/dcim/devices/",
                json={
                    "name": fields["hostname"],
                    "serial": fields["sn"],
                    "device_role": role_unknwon,
                    "platform": pl_id,
                    "site": site_id,
                    "device_type": dt_id,
                },
            )

        await self.source.update(items, callback, _create_task)

    async def update_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):

        api: NetboxClient = self.source.client

        # ensure that the 'ipaddrs' Collection is in the cache.

        if (cached_ipaddrs := self.cache.get("ipaddrs")) is None:
            # we need to fetch all ipaddrs from Netbox so that they can be processed
            # in the create task function below.

            # ipaddr_list = [
            #     (self.inventory[key]['hostname'], item.fields['ipaddr'])
            #     for key, item in changes.items()
            #     if 'ipaddr' in item.fields
            # ]
            #
            # cached_ipaddrs = await self._ensure_ipaddrs(ipaddr_list)

            whoami = self.__class__.__name__
            print(f"SKIP: {whoami}.update_changes requires 'ipaddrs' in cache")
            return

        # TODO: Hacking the cache to remove the pflen because the IP in the device
        #       record does not have that information from IPF.

        kex_lkup = {
            rec["address"].split("/")[0]: rec
            for rec in cached_ipaddrs.source_record_keys.values()
        }

        def _create_task(key, fields: dict):
            """ key is the seriali number """
            patch_payload = {}

            if (ipaddr := fields.get("ipaddr")) is not None:

                if (nb_rec := kex_lkup.get(ipaddr)) is None:
                    print(f"SKIP: ipaddr {ipaddr} not in device cache.")
                    return None

                patch_payload["primary_ip4"] = nb_rec["id"]

            if not len(patch_payload):
                return None

            dev_id = self.source_record_keys[key]["id"]

            # Note: no slash between the base URL and the dev_id since the
            #       base url has a slash-suffix

            return api.patch(url=f"{_DEVICES_URL}{dev_id}/", json=patch_payload)

        await self.source.update(items, callback, creator=_create_task)

    async def delete_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        raise NotImplementedError()
