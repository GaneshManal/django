# from django.shortcuts import render
# import django
import json
import os
from django.http import HttpResponse, HttpResponseNotFound, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from .models import Template

from deepInsight.util import get_logger
from deepInsight.watchdog.base_monitor import *


class TemplateManager:
    __name__ = 'TemplateManager'
    logger = get_logger(__name__)

    def __init__(self, id):
        self.id = id

    @classmethod
    def list(self, request):
        self.logger.info('API Call - List Templates.')
        all_templates = Template.objects.all()

        if not len(all_templates):
            self.logger.debug('Application has no template Added.')

        response = []
        for x_temp in all_templates:
            response.append({"Id": x_temp.id, "State": x_temp.state,
                             "Cluster": x_temp.cluster, "Created": x_temp.created.isoformat()})
        return HttpResponse(json.dumps(response))

    @classmethod
    @csrf_exempt
    def show(self, request, id):
        self.logger.info('API Call - Show Template.')
        x_temp = Template.objects.filter(id=id)

        response = dict()
        if len(x_temp) == 0:
            self.logger.error('Template not found for id - %s.', id)
            response['status'] = 'In-Valid Template-Id.'
            return HttpResponseNotFound(json.dumps(response))
        else:
            with open(settings.DATA_DIR + os.path.sep + id) as f:
                response = json.load(f)
        return HttpResponse(json.dumps(response))

    @classmethod
    @csrf_exempt
    def add(self, request):
        self.logger.info('API Call - Add a new Template.')
        # Read input and update file to temp directory.
        response = dict()
        filedata = request.FILES.get('file', None)

        if not filedata:
            self.logger.error('Invalid input ( file not attached as File-Filedata Key-Value pair ).')
            response['Status'] = 'In-valid input.'
            return HttpResponseBadRequest(json.dumps(response))

        tid = os.urandom(4).encode('hex')
        filepath = settings.DATA_DIR + os.path.sep + tid

        destination = open(filepath, 'w')
        for chunk in filedata.chunks():
            destination.write(chunk)
        destination.close()

        try:
            with open(filepath, 'r') as f:
                filedata = json.load(f)
        except:
            self.logger.error('Invalid input ( Not JSON ).')
            os.remove(filepath)
            response['Status'] = 'In-valid input.'
            return HttpResponseBadRequest(json.dumps(response))

        self.logger.debug('Template file successfully Stored.')

        # Update record to the database.
        record = Template()
        record.id, record.state = tid, 'Init'
        record.cluster = filedata.get('name', 'Not Given')
        response['id'] = tid
        record.save()
        self.logger.debug('Template record successfully Added.')
        self.logger.info('Template successfully Added.')
        return HttpResponse(json.dumps(response))

    @classmethod
    @csrf_exempt
    def status(self, request, tid):
        self.logger.info('API Call - Template Status.')
        x_temp = Template.objects.filter(id=tid, state='Active')

        response, template = dict(), dict()
        if len(x_temp) == 0:
            self.logger.error('Template currently not Active.')
            response['status'] = 'Template Not Active.'
            return HttpResponse(json.dumps(response))
        else:
            with open(settings.DATA_DIR + os.path.sep + tid) as f:
                template = json.load(f)

        response = {}
        sub_templates = [key for key, val in template.get('sub_template')[0].iteritems() if key != 'name']
        for watchdog_class in BaseMonitor.__subclasses__():
            if watchdog_class.NAME in sub_templates:
                obj = watchdog_class(tid)
                response[watchdog_class.NAME] = obj.check_status()
            else:
                continue
        return HttpResponse(json.dumps(response))

    @classmethod
    def delete(self, request, id):
        self.logger.info('API Call - Delete Template.')
        x_temp = Template.objects.filter(id=id)

        response = dict()
        if len(x_temp) == 0:
            self.logger.error('Template does not exist for id - %s.', id)
            response['status'] = 'In-Valid Template-Id.'
            return HttpResponseNotFound(json.dumps(response))
        else:
            # Delete stored file.
            filepath = settings.DATA_DIR + os.path.sep + id
            if os.path.isfile(filepath):
                os.remove(filepath)
                self.logger.debug('Template file successfully Deleted.')

            # Delete database Record.
            x_temp.delete()
            self.logger.debug('Template record successfully Deleted.')
            response['status'] = 'Template Deleted.'
        return HttpResponse(json.dumps(response))
