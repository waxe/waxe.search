import os
import sys

from sqlalchemy import engine_from_config

from pyramid.paster import (
    get_appsettings,
    setup_logging,
)

import xmltool.cache
xmltool.cache.CACHE_TIMEOUT = 3600

from waxe.core import browser
from waxe.search import elastic

from waxe.core.models import (
    DBSession,
    UserConfig,
)
import sqla_taskq.models as taskqm

from elasticsearch import Elasticsearch


def usage(argv):
    cmd = os.path.basename(argv[0])
    print('usage: %s <config_uri>\n'
          '(example: "%s development.ini")' % (cmd, cmd))
    sys.exit(1)


def main(argv=sys.argv):
    if len(argv) != 2:
        usage(argv)
    config_uri = argv[1]
    setup_logging(config_uri)
    settings = get_appsettings(config_uri, name="waxe")
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)
    taskqm.DBSession.configure(bind=engine)

    if 'waxe.search.url' not in settings:
        print 'Elasticsearch url not defined in your conf'
        sys.exit(1)

    url = settings['waxe.search.url']

    index_name_prefix = settings.get('waxe.search.index_name_prefix')

    for uc in UserConfig.query.all():
        if not uc.root_path:
            continue
        if not os.path.exists(uc.root_path):
            continue
        DBSession.add(uc)
        user_index_name = uc.user.get_search_dirname()
        index_name = index_name_prefix + user_index_name
        # TODO: support another extensions for the search
        # TODO: Get extensions from the settngs
        extensions = ['.xml']
        paths = browser.get_all_files(extensions, uc.root_path, uc.root_path)[1]
        # elastic.incremental_index(url, index_name, paths, uc.root_path)
        taskqm.Task.create(
            elastic.incremental_index,
            [url, index_name, paths, uc.root_path],
            owner=str(uc.user.iduser),
            unique_key='search_%i' % uc.user.iduser)


def delete(argv=sys.argv):
    if len(argv) != 2:
        usage(argv)
    config_uri = argv[1]
    setup_logging(config_uri)
    settings = get_appsettings(config_uri, name="waxe")
    engine = engine_from_config(settings, 'sqlalchemy.')
    DBSession.configure(bind=engine)
    taskqm.DBSession.configure(bind=engine)

    if 'waxe.search.url' not in settings:
        print 'Elasticsearch url not defined in your conf'
        sys.exit(1)

    url = settings['waxe.search.url']

    index_name_prefix = settings.get('waxe.search.index_name_prefix')

    for uc in UserConfig.query.all():
        if not uc.root_path:
            continue
        DBSession.add(uc)
        user_index_name = uc.user.get_search_dirname()
        index_name = index_name_prefix + user_index_name
        client = Elasticsearch(url)
        client.indices.delete(index_name, ignore=404)


if __name__ == '__main__':
    # delete()
    main()
