# -*- coding: utf-8 -*-

from datetime import datetime
from django.core.files import File
from django.core.management.base import BaseCommand
from django.contrib.contenttypes.models import ContentType
from django.utils.dateparse import parse_datetime
from json import load
from optparse import make_option
from parsesync import to_snake_case
from parsesync.models import ParseModel
from urllib import urlretrieve
import os

"""

Example: ./manage.py importparsejson /Users/username/some_export/_User.json --model=CustomUser

"""

class Command(BaseCommand):
    help = 'Sync exported data from Parse to Django. Expects path to file as argument'
    verbose = False

    # Django < 1.7- support
    option_list = BaseCommand.option_list + (
        make_option('--model', help='Override figuring out model name by filename'),
        make_option('--verbose', help='Emit noise'),
    )

    # Django 1.8+ support
    def add_arguments(self, parser):
        parser.add_argument('--model', help='Override figuring out model name by filename')
        parser.add_argument('--verbose', help='Emit noise')

    def handle(self, *args, **options):
        filename = args[0]

        # based on file name or override, filter the model
        model_filter = options.get('model')
        self.verbose = options.get('verbose')
        if not model_filter:
            model_filter = os.path.basename(filename).split('.')[0]

        # open file
        with open(filename, 'r') as f:
            data = load(f)

        # import all entries
        # dependencies must be identified and synced first
        model_filter = ''.join(model_filter.lower().split(' '))
        content_types = ContentType.objects.filter(model=model_filter)

        for content_type in content_types:
            model = content_type.model_class()
            if issubclass(model, ParseModel):
                results = data['results']
                self.save(model, model_filter, results)

    def verbose_print(self, thing):
        if self.verbose:
            print thing

    def save(self, model, model_name, results):
        for item in results:
            object_id = item.get('objectId')
            updated_at = item.get('updatedAt')
            files = []

            try:
                instance = model.objects.get(object_id=object_id)
                self.verbose_print('\tUpdating Django %s.%s, last updated at %s...' % (model_name, object_id, updated_at))
            except model.DoesNotExist:
                self.verbose_print('\tCreating Django %s.%s, last updated at %s...' % (model_name, object_id, updated_at))
                instance = model()

            for key, value in item.items():
                snake_key = to_snake_case(key)

                if key == 'authData':
                    continue # TODO: handle facebook auth data

                if type(value) != dict:
                    setattr(instance, snake_key, value)
                elif value['__type'] == 'Date':
                    conv_value = parse_datetime(value['iso'])
                    setattr(instance, snake_key, conv_value)
                elif value['__type'] == 'File':
                    if 'url' in value:
                        dl_file = urlretrieve(value['url'])
                        files.append([getattr(instance, snake_key), value['name'], File(open(dl_file[0]))])
                elif value['__type'] == 'Pointer':
                    setattr(instance, '%s_id' % value['className'].lower(), value['objectId'])
                else:
                    print 'Unhandled: %s' % value

            # avoiding Parse update, saving only locally
            instance.save_to_parse = False
            try:
                instance.save()
            except Exception, e:
                print 'Error [%s] ocurred while saving your content' % e

            # saving and associating files
            for f in files:
                f[0].save(f[1], f[2])
