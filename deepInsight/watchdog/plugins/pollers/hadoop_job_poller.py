'''
Mapr Hadoop job Poller to poll job level details

Prerequisites: Install python package using following command
                    pip install yarn-api-client==0.2.3
Parameter:
            {
            'resource_manager_address': <mapr-cluster-ip>,
            'port': <mapr-cluster-port>, # Port where cluster webserver running
            'application_tag': <tag>, # Fetch job based on tag provided while starting application
            'application_ids : <job-id-list>, # Fetch job based on tag provided while running application
            'application_names : <job-name-list>, # Fetch job based on tag provided while running application
            'application_status: 'running' # Fetch running jobs only 
            }
'''


from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager
import json
import time
from deepInsight.util import get_logger

logger = get_logger("Mapr Hadoop Job Poller")


class HadoopJobPoller:
    
    _NAME_ = "Mapr Hadoop Job Poller"

    def __init__(self, config):
        logger.info('Initialise {}'.format(self.get_name()))
        self.config = config
        self.configure()
        self.result = []

    def load_config(self, config):
        logger.debug('Loading config: {}'.format(config))
        self.config = config
        self.configure()

    def get_name(self):
        return HadoopJobPoller._NAME_

    def configure(self):
        resource_manager_address = self.config.get('resource_manager_address')
        port = self.config.get('port')
        if port:
            self.resource_manager = ResourceManager(
                address=resource_manager_address, port=port)
            self.app_master = ApplicationMaster(
                address=resource_manager_address, port=port)
        else:
            self.resource_manager = ResourceManager(address=resource_manager_address)
            self.app_master = ApplicationMaster(address=resource_manager_address)

        self.application_ids = self.config.get('application_ids')
        self.application_status = self.config.get('application_status')
        self.application_tags = self.config.get('application_tags')
        self.application_names = self.config.get('application_names')
        self.application_status_list=[]

    def __update_result(self, result={}):
        result.update({'time':time.time()})
        self.result.append(result)

    def poll(self):
        logger.info("Starting {} poll".format(self.get_name()))
        try:
            self.__application_details()
            success_status = {
                "status": "COMPLETED",
                "status_message": "Hadoop Job poll completed successfully",
                "applications_status":self.application_status_list
            }
            logger.info("Successfully completed {} poll".format(self.get_name()))
            return self.result, success_status

        except Exception as e:
            logger.error("Exception in {} poll :{}".format(self.get_name(), str(e)))
            exception_status = {
                "status": "EXCEPTION",
                "status_message": str(e)
            }

            return self.result, exception_status

    def __application_details(self):
        self.cluster_id = self.resource_manager.cluster_information().data.get('clusterInfo').get('id')
        app_list = self.resource_manager.cluster_applications().data.get('apps')

        if app_list:
            for app in app_list.get('app'):

                if app.get('state').lower() == 'running':
                    jobs = (self.app_master.jobs(
                        app.get('id')).data.get('jobs').get('job'))
                    result_job_list = []
                    for job in jobs:
                        task = self.app_master.job_tasks(
                            app.get('id'), job.get('id')).data
                        job.update(task)
                        result_job_list.append(job)
                    app.update({"jobs": result_job_list})

                # Condition fetch application based on id
                if (self.application_names is not None) and (app.get('name') not in self.application_names):
                    continue

                # Condition fetch application based on id
                if (self.application_ids is not None) and (app.get('id') not in self.application_ids):
                    continue

                # Condition fetch application based on id
                if (self.application_status is not None) and (app.get('state').lower() != self.application_status):
                    continue

                # Condition fetch application based on id
                if (self.application_tags is not None) and (app.get('applicationTags') not in self.application_tags):
                    continue
                app_status = {'application_id': app.get('id'), 'application_name':app.get('name'),'status': app.get('state')}
                self.application_status_list.append(app_status)
                self.__update_result(app)
#         app_results = {'clusterId': self.cluster_id,
#                        'applications': app_result_list}
#         #self.__update_result(app_result_list)
#         #self.result = app_result_list

# Function Called by Poller Controller


def poll(meta={}, state={}):
    return HadoopJobPoller(meta).poll()

'''
if __name__ == '__main__':
    config = dict(
        {'resource_manager_address': 'node116', 'port': 8088, 'application_status': 'running'})

    print config
    # yarn_plugin = HadoopJobPoller())
    result, status = poll(meta=config)  # yarn_plugin.poll()
    print "RESULT :" + json.dumps(result)
    print "STATUS :" + json.dumps(status)
'''
