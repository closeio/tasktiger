#!/usr/bin/env python
"""
Safely scan Redis instance and report key stats.

Can also set the TTL for keys to facilitate removing data.
"""
import signal
import sys
import time

import click
import redis

loop = True


def signal_handler(signum, frame):
    """Signal handler."""

    global loop
    print("Caught ctrl-c, finishing up.")
    loop = False


def get_size(client, key, key_type):
    """Get size of key."""

    size = -1
    if key_type == "string":
        size = client.strlen(key)
    elif key_type == "zset":
        size = client.zcard(key)
    elif key_type == "set":
        size = client.scard(key)
    elif key_type == "list":
        size = client.llen(key)
    elif key_type == "hash":
        size = client.hlen(key)

    return size


@click.command()
@click.option("--file", "file_name", default="redis-stats.log")
@click.option("--match", default=None)
@click.option(
    "--ttl",
    "set_ttl",
    default=None,
    type=click.INT,
    help="Set TTL if one isn't already set (-1 will remove TTL)",
)
@click.option("--host", required=True)
@click.option("--port", type=click.INT, default=6379)
@click.option("--db", type=click.INT, default=0)
@click.option("--delay", type=click.FLOAT, default=0.1)
@click.option("--print", "print_it", is_flag=True)
def run(host, port, db, delay, file_name, print_it, match, set_ttl=None):
    """Run scan."""

    if set_ttl is not None and match is None:
        print("You must specify match when setting TTLs!")
        sys.exit(1)

    client = redis.Redis(host=host, port=port, db=db)

    if match:
        print(f"Scanning redis keys with match: {match}\n")
    else:
        print("Scanning all redis keys\n")

    # This is a string because we want the `while` below to run, and the string
    # will be correctly interpreted by `client.scan` as a request for a new
    # cursor.
    cursor = "0"

    signal.signal(signal.SIGINT, signal_handler)

    with open(file_name, "w") as log_file:
        while cursor != 0 and loop:
            cursor, data = client.scan(cursor=cursor, match=match)

            for key in data:
                key_type = client.type(key)
                size = get_size(client, key, key_type)

                ttl = client.ttl(key)
                new_ttl = None
                if set_ttl == -1:
                    if ttl != -1:
                        client.persist(key)
                    new_ttl = -1
                elif set_ttl is not None and ttl == -1:
                    # Only change TTLs for keys with no TTL
                    client.expire(key, set_ttl)
                    new_ttl = set_ttl

                line = f"{key} {key_type} {ttl} {new_ttl} {size}"
                log_file.write(line + "\n")
                if print_it:
                    print(line)

            log_file.flush()
            time.sleep(delay)


if __name__ == "__main__":
    run()
