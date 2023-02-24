from .check_tag import Check_Tag


def check_if_version3(infile):
    if not Check_Tag(infile, "SKIM:V03"):
        raise ValueError("Not Version 3")
