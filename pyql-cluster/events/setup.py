async def run(server):
    print("running events")

    from events.health import health
    health.run(server)

    from events.jobs import jobs
    jobs.run(server)

    from events.cluster import cluster
    cluster.run(server)