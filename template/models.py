from __future__ import unicode_literals

from django.db import models


# Create your models here.
class Template(models.Model):
    TEMPLATE_STATES = (
        ('I', 'Init'),
        ('A', 'Active'),
        ('S', 'Stopped'),
        ('D', 'Deleted'),
    )

    id = models.CharField(max_length=60, primary_key=True)
    state = models.CharField(max_length=1, choices=TEMPLATE_STATES)
    cluster = models.CharField(max_length=60, blank=True)
    created = models.DateTimeField(auto_now_add=True, blank=True)

    def __str__(self):
        return self.id
