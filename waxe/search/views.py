import os

from pyramid.view import view_config
import pyramid.httpexceptions as exc
from waxe.core.views.base import BaseUserView
from waxe.core import browser
from . import elastic


class SearchView(BaseUserView):

    @view_config(route_name='search_json')
    def search(self):
        user_index_name = self.get_search_dirname()
        if not user_index_name:
            raise exc.HTTPInternalServerError('The search is not available')

        search = self.req_get.get('search')
        if not search:
            raise exc.HTTPClientError('Nothing to search')

        filetype = self.req_get.get('filetype')
        tag = self.req_get.get('tag')

        path = self.req_get.get('path') or ''
        page_num = self.req_get.get('page') or 1
        try:
            page_num = int(page_num)
        except ValueError:
            page_num = 1

        abspath = None
        if path:
            abspath = browser.absolute_path(path, self.root_path)

        url = self._get_search_url()
        index_name = self._get_search_index()
        res, nb_hits = elastic.do_search(
            url, index_name, search, abspath=abspath, ext=filetype, tag=tag,
            page=page_num)

        lis = []
        if res:
            for dic in res:
                newdic = {}
                newdic['path'] = dic['_source']['relpath']
                newdic['name'] = os.path.basename(newdic['path'])
                highlight = dic['highlight']
                newdic['excerpts'] = []
                for k, v in highlight.iteritems():
                    key = ' > '.join(k.split('.')[1:])
                    newdic['excerpts'].append((key, v))
                lis += [newdic]
        return {
            'results': lis,
            'nb_items': nb_hits,
            'items_per_page': elastic.HITS_PER_PAGE,
        }

    @view_config(route_name='search_path_complete_json')
    def search_path_complete(self):
        dirname = self.get_search_dirname()
        if not dirname:
            raise exc.HTTPInternalServerError('The search is not available')

        search = self.req_get.get('search') or ''
        if not search:
            raise exc.HTTPClientError('Nothing to search')

        url = self._get_search_url()
        index_name = self._get_search_index()
        return elastic.path_completion(url, index_name, search)


def includeme(config):
    # TODO: remove this hardcoded prefix
    prefix = '/api/1/account/{login}/'
    config.add_route('search_json', prefix + 'search/search.json')
    config.add_route('search_path_complete_json',
                     prefix + 'search/path-complete.json')
    config.scan(__name__)
