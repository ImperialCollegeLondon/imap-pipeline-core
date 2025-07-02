from imap_mag.util.HKPacket import HKPacket


def convert_packet_to_spdf_name(packet: str | HKPacket) -> str:
    """Convert HK packet name to SPDF name, used, e.g., in folder structures."""

    if isinstance(packet, HKPacket):
        packet = packet.packet

    return packet.lower().strip("mag_").replace("_", "-")
