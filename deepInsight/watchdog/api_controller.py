from base_monitor import *
from collectd_scan import CollectdPluginManager
from fluentd_logger import FluentdPluginManager
from poller_watch import PollerManager
from django.views.decorators.csrf import csrf_exempt

from template.views import Template
from django.http import HttpResponse, HttpResponseBadRequest
from deepInsight.util import get_logger
from django.conf import settings


class ClusterMonitor:
    logger = get_logger('ClusterMonitor')

    def __init__(self):
        pass

    @classmethod
    def monitor(self, request, tid, oper):
        self.logger.info('API Call - Cluster Monitoring. ( operation = %s )', oper)
        self.logger.debug('Input template id - %s', tid)

        response, curr_temp = dict(), dict()
        curr_temp = Template.objects.filter(id=tid)
        if len(curr_temp) == 0:
            response['status'] = 'Template Not Found.'
            self.logger.error('Template Not Found')
            return HttpResponseBadRequest(json.dumps(response))
        else:
            if oper.lower() == 'start':
                active_temps = Template.objects.filter(state='Active')
                for item in active_temps:
                    if item.id == tid:
                        response['status'] = 'Template Already Active.'
                        self.logger.error('Monitoring is already started for this template.')
                        return HttpResponse(json.dumps(response))
                    else:
                        item.state = 'Stopped'
                        item.save()
                        self.logger.warning('Template: %s - Monitoring overwritten by Template: %s.' % (item.id, tid))

            if oper.lower() == 'stop':
                not_active_temps = Template.objects.exclude(state='Active')
                for item in not_active_temps:
                    if item.id == tid:
                        response['status'] = 'Template Not Active.'
                        self.logger.error('Monitoring is not started for this template.')
                        return HttpResponse(json.dumps(response))

        template = {}
        with open(settings.DATA_DIR + os.path.sep + tid) as f:
            template = json.load(f)
        sub_templates = [key for key, val in template.get('sub_template')[0].iteritems() if key != 'name' ]
        self.logger.debug('Available sub-templates in template - %s', str(sub_templates))

        watchdogs = {}
        for watchdog_class in BaseMonitor.__subclasses__():
            watch_type = watchdog_class.NAME
            if watch_type in sub_templates:
                self.logger.debug('Initializing Object - %s', watch_type)
                watchdogs[watch_type] = watchdog_class(tid)
                self.logger.debug('Initialization Complete.')
            else:
                self.logger.warning('%s - Not available in Template.', watch_type)
                continue

        '''
        watchdogs = {}
        for item in sub_templates:
            if item == 'loggers':
                watchdogs['loggers'] = FluentdPluginManager(tid)
                self.logger.debug('Initializing Object Loggers.')

            if item == 'collectors':
                watchdogs['collectors'] = CollectdPluginManager(tid)
                self.logger.debug('Initializing Object Collectors.')

            if item == 'pollers':
                watchdogs['pollers'] = PollerManager(tid)
                self.logger.debug('Initializing Object Pollers.')
        '''

        # Provision the Template for each of the watch dogs.
        response = dict()
        self.logger.debug('Perform Operation(%s) for : %s' % (oper.upper(), str(watchdogs.keys())))
        for name, obj in watchdogs.iteritems():
            status = obj.deploy(oper.lower())
            self.logger.debug('%s: Provision Status: %s' % (name, json.dumps(status)))
        # Update template status as Active.
        if oper.lower() == 'start':
            curr_temp[0].state = 'Active'
            curr_temp[0].save()

        elif oper.lower() == 'stop':
            curr_temp[0].state = 'Stopped'
            curr_temp[0].save()

        elif oper.lower() == 'teardown':
            curr_temp[0].delete()
            self.logger.debug('Template Record Deleted.')
            response["Status"] = "Monitoring " + oper.capitalize() + "ed Successfully"
            return HttpResponse(json.dumps(response))
        else:
            pass

        self.logger.info("Template State Changed.")
        # Create Response
        response["Status"] = "Monitoring " + oper.capitalize() + "ed Successfully"
        response["template_id"] = tid
        self.logger.info("Hadoop Monitoring " + oper.capitalize() + "ed Successfully.")
        return HttpResponse(json.dumps(response))


class JobMonitor:
    logger = get_logger('JobMonitor')

    def __init__(self):
        pass

    @classmethod
    @csrf_exempt
    def perform(self, request, tid, oper):
        self.logger.info('API Call - Job Monitoring. ( operation = %s )', oper)
        pass

        response = dict()
        curr_temp = Template.objects.filter(id=tid)
        if len(curr_temp) == 0:
            response['status'] = 'Template Not Found.'
            self.logger.error('Template Not Found')
            return HttpResponseBadRequest(json.dumps(response))

        data = json.loads(request.body)
        self.logger.info('Data Input: %s', json.dumps(data))
        pm_obj = PollerManager(tid)
        pm_obj.start_job(data)


