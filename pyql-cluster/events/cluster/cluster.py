async def run(server):

    def get_factory_coro(job_type):
        async def job_check_and_run():
            await server.job_check_and_run(job_type)
        f = job_check_and_run
        f.__name__ = f'{job_type}_{f}'
        return f

    # cron job run
    server.tasks.append(
        get_factory_coro('cron')
    )
    # sync job run
    server.tasks.append(
        get_factory_coro('syncjob')
    )
    # job run 
    server.tasks.append(
        get_factory_coro('job')
    )