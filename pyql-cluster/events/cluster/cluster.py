async def run(server):

    def get_factory_coro(job_type):
        async def job_check_and_run():
            await server.job_check_and_run(job_type)
        f = job_check_and_run
        f.__name__ = f'{job_type}_{f}'
        return f

    # cron job run
    @server.cron(15)
    async def check_cron_jobs():
        await server.job_check_and_run('cron')

    # sync job run
    @server.cron(15)
    async def check_sync_jobs():
        await server.job_check_and_run('syncjob')

    # job run 
    @server.cron(15)
    async def check_jobs():
        await server.job_check_and_run('job')