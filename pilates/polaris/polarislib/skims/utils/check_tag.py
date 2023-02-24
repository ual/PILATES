import struct


def Check_Tag(file, check_val, rewind=False):
    size = len(check_val)
    read_val = file.read(size)
    if rewind:
        file.seek(-size, 1)

    # check if at end of file
    if len(read_val) != size:
        return False

    # if not, read in tag
    tag = struct.unpack(f"<{size}s", read_val)[0]

    # check against expected value and exit if requested
    return tag.decode("utf-8") == check_val
