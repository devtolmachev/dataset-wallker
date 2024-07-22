import humanize


def get_human_readable_size(size: int):
    return humanize.naturalsize(size, binary=False)
