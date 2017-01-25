import urllib2
import anyjson
import logging

from datetime import datetime
from itertools import groupby

from airflow.operators.sensors import BaseSensorOperator
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

YARN_HOST = os.getenv("YARN_HOST", False)
HDFS_HOST = os.getenv("HDFS_HOST", False)


def log(text):
    logging.info(text)


def yarn_kill_app(app_id):
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(\
      url='{0}/ws/v1/cluster/apps/{1}/state'.format(YARN_HOST, app_id),\
      data='{"state":"KILLED"}',\
      headers={"Content-Type" : "application/json"})
    request.get_method = lambda: 'PUT'
    response = urllib2.urlopen(request)
    response.read()


def yarn_apps(status=[]):
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(\
      url='{0}/ws/v1/cluster/apps?states={1}'.format(YARN_HOST,",".join(status)),\
      headers={"Content-Type" : "application/json"})
    response = urllib2.urlopen(request)
    return anyjson.deserialize(response.read())


def yarn_apps_running_and_accepted():
    return yarn_apps(["RUNNING", "ACCEPTED"])


def yarn_get_app_running_or_accepted(name, user):
    results = yarn_apps_running_and_accepted()["apps"]["app"]
    app = filter(lambda a: a['name'] == name and a["user"] == user, results)
    return app


class YarnAppNotRunningSensor(BaseSensorOperator):
    template_fields = tuple()

    @apply_defaults
    def __init__(self, app_name, app_user, *args, **kwargs):
        super(YarnAppNotRunningSensor, self).__init__(*args, **kwargs)
        self.timeout = 0
        self.poke_interval = 1
        self.soft_fail = True
        self.app_name = app_name
        self.app_user = app_user

    def poke(self, context):
        log("Schedule interval: " + str(context["task"].schedule_interval))
        log("Execution date: " + str(context["execution_date"]))
        log("Timeout: " + str(self.timeout))
        app = yarn_get_app_running_or_accepted(self.app_name, self.app_user)
        log("App found: " + str(app))
        return not app


class YarnAppTakingTooMuchTime(BaseSensorOperator):
    template_fields = tuple()

    @apply_defaults
    def __init__(self, app_name, app_user, max_minutes, *args, **kwargs):
        super(YarnAppTakingTooMuchTime, self).__init__(*args, **kwargs)
        self.timeout = 0
        self.poke_interval = 1
        self.soft_fail = True
        self.app_name = app_name
        self.app_user = app_user
        self.max_minutes = max_minutes

    def poke(self, context):
        apps = yarn_get_app_running_or_accepted(self.app_name, self.app_user)
        for app in apps:
            elapsed_minutes = app["elapsedTime"] / 60000
            if self.max_minutes < elapsed_minutes:
                return True
        return False


class YarnKillAppOperator(BaseOperator):

    @apply_defaults
    def __init__(self, app_name, app_user, *args, **kwargs):
        super(YarnKillAppOperator, self).__init__(*args, **kwargs)
        self.app_name = app_name
        self.app_user = app_user

    def execute(self, context):
        apps = yarn_get_app_running_or_accepted(self.app_name, self.app_user)
        log("apps: " + str(apps))
        for app in apps:
            app_id = app['id']
            log("killing " + app_id)
            yarn_kill_app(app_id)


class YarnAvoidDuplicatedApp(BaseOperator):

    @apply_defaults
    def __init__(self, app_prefix, app_user, *args, **kwargs):
        super(YarnAvoidDuplicatedApp, self).__init__(*args, **kwargs)
        self.app_prefix = app_prefix
        self.app_user = app_user

    def execute(self, context):
        apps = yarn_apps_running_and_accepted()["apps"]["app"]
        filterfunc = lambda a: a['name'].startswith(self.app_prefix) and a["user"] == self.app_user
        filtereds = filter(filterfunc, apps)
        mapped = [ (a["name"], a["id"], a["elapsedTime"]) for a in filtereds ]
        keygroup = lambda a: a[0]
        grouped = groupby(sorted(mapped, key=keygroup), keygroup)
        for app in grouped:
            self.kill_new_ones(list(app[1]))

    def kill_new_ones(self, apps):
        if len(apps) > 1:
            log("found duplicated jobs for " + apps[0][0])
            sorts = sorted(apps, key=lambda a: a[2])
            sliced = sorts[:len(sorts)-1]
            log("leaving original app: " + sorts[-1][1])
            for tokill in sliced:
                log("killing duplicated app: " + tokill[1])
                yarn_kill_app(tokill[1])


def hdfs_file_info(path):
    norm_path = path.replace("hdfs://", "")
    api = "{0}/webhdfs/v1{1}?op=LISTSTATUS".format(HDFS_HOST, norm_path)
    body = urllib2.urlopen(api).read()
    return anyjson.deserialize(body)


def hdfs_file_modified_date(path):
    result = hdfs_file_info(path)
    return result["FileStatuses"]["FileStatus"][0]["modificationTime"]


class HdfsFileHasChanged(BaseSensorOperator):
    template_fields = tuple()

    @apply_defaults
    def __init__(self, file_path, *args, **kwargs):
        super(HdfsFileHasChanged, self).__init__(*args, **kwargs)
        self.timeout = 0
        self.poke_interval = 1
        self.soft_fail = True
        self.file_path = file_path

    def poke(self, context):
        key = self.dag.dag_id + ":" + self.file_path +":last_modification"
        log("key: " + str(key))
        last = self.safe_get_var(key)
        new = hdfs_file_modified_date(self.file_path)
        log("last: " + str(last))
        log("new: " + str(new))
        Variable.set(key, new)
        result = last and int(new) > int(last)
        log("result: " + str(result))
        return result

    def safe_get_var(self, key):
      try:
        return Variable.get(key)
      except:
        return None
