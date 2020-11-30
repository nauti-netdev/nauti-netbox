from nauti.auditor import Auditor
from nauti.igather import iawait
from nauti.log import get_logger


class NetboxWithDeviceAuditor(Auditor):
    """
    This auditor is used when Netbox is a target and when the collection
    requires the device fetch filter; as is the case with interfaces, ipaddrs,
    and other device related collections.
    """

    async def fetch_target(self):
        log = get_logger()
        devices = {item["hostname"] for item in self.origin.items.values()}
        tasks = [self.target.fetch(device=device) for device in devices]

        ident = f"{self.target.source.name}/{self.name}"
        log.info(f"Fetching {ident} collection ...")

        # TODO: remove hardcode limit to something configuraable
        await iawait(tasks, limit=100)

        log.info(f"Fetched {ident}, fetched {len(self.target.source_records)} records.")
        self.target.make_keys(with_filter=self.target_key_filter)
