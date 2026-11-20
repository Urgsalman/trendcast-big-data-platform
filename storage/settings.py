"""Validate storage destinations before starting a distributed job."""

from urllib.parse import urlsplit


def validate_destinations(output: str, checkpoint: str) -> None:
    paths = []
    for value in (output, checkpoint):
        parsed = urlsplit(value)
        if parsed.scheme != "hdfs" or not parsed.netloc:
            raise ValueError("Output and checkpoints must use an absolute hdfs:// URI")
        if parsed.query or parsed.fragment or not parsed.path.startswith("/trendcast/"):
            raise ValueError(
                "Storage paths must be below /trendcast/ without query or fragment"
            )
        components = parsed.path.strip("/").split("/")
        if any(part in (".", "..", "") for part in components):
            raise ValueError(
                "Storage paths must not contain relative or empty components"
            )
        paths.append((parsed.netloc, parsed.path.rstrip("/")))
    if paths[0][0] != paths[1][0]:
        raise ValueError("Output and checkpoints must use the same HDFS cluster")
    left, right = (path for _, path in paths)
    if left == right or left.startswith(right + "/") or right.startswith(left + "/"):
        raise ValueError("Output and checkpoint directories must be separate")
