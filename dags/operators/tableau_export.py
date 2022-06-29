from datetime import datetime
from typing import Optional, Dict
import logging
import os
import shutil
import tempfile

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.tableau.hooks.tableau import TableauHook


def download_pdf(server, tempdir, pdf_req_options, view):  # -> Filename to downloaded pdf
    logging.info(f"Exporting {view.name}")
    destination_filename = os.path.join(tempdir, view.id)
    server.views.populate_pdf(view, pdf_req_options)
    with open(destination_filename, 'wb') as f:
        f.write(view.pdf)
    return destination_filename

def get_wb_url(tableau_conn, view, view_filters: Dict[str,str]) -> str:
    '''Get tableau workbook url with filter parameters

    Args:
        tableau_conn: a connection to tableau
        view: a view in the workbook
        view_filters (dict): filters to apply to view

    Returns:
        str: url to specified view with filters applied
    '''
    from urllib.parse import urlencode
    filter_params = urlencode(view_filters)
    tableau_base_url = tableau_conn.host
    wb_url = f'{tableau_base_url}/#/views/{view.content_url}?{filter_params}'
    wb_url = wb_url.replace('/sheets/','/')
    return wb_url

def _combine_into(dest_pdf, filename):  # -> None
    dest_pdf.append(filename)
    return dest_pdf


class TableauExportWbOperator(BaseOperator):
    """Export Tableau workbook as PDFs

    Args:
        find (str): [description]
        output_path (str):
            Path to output pdf.
        match_with (str, optional):
            The reference of resource that will receive the action.
            Defaults to 'id'.
        metadata (dict, optional):
            Metadata to be added to result pdf.
            Allowed keys:
                '/Author' – who created the document
                '/CreationDate' – the date and time when the document was originally created
                '/Creator' – the originating application or library
                '/Subject' – what is the document about
                '/Title' –  the title of the document
                '/Keywords' – keywords can be comma-separated
                '/ModDate' -the latest modification date and time
            Allowed values: any string
            Defaults to None.
        page_type (str, optional):
            Defaults to 'A3'.
        orientation (str, optional):
            Defaults to 'LandScape'.
        max_age (int, optional):
            The maximum number of minutes the rendered PDF will be cached
            on the server before being refreshed. Defaults to 1.
        view_filters
            View filters that apply to all exports. Defaults to None.
        site_id (Optional[str], optional):
            The id of the site where the workbook belongs to.
            Defaults to None.
        blocking_refresh (bool, optional):
            By default will be blocking means it will wait until it has
            finished. Defaults to True.
        check_interval (float, optional):
            Time in seconds that the job should wait in between each instance
            state checks until operation is completed. Defaults to 20.
        tableau_conn_id (str, optional):
            The Tableau Connection id containing the credentials to
            authenticate to the Tableau Server. Defaults to 'tableau_default'.
    """

    def __init__(
            self,
            find: str,
            output_path: str,
            metadata: Optional[dict] = None,
            match_with: str = 'id',
            page_type: str = 'A3',
            orientation: str ='LandScape',
            max_age: int = 1,
            view_filters: Optional[Dict[str, str]] = None,
            site_id: Optional[str] = None,
            blocking_refresh: bool = True,
            check_interval: float = 20,
            tableau_conn_id: str = 'tableau_default',
            **kwargs,
        ) -> None:

        super().__init__(**kwargs)
        self.find = find
        self.output_path = output_path
        self.metadata = metadata or {}
        self.match_with = match_with
        self.page_type = page_type
        self.orientation = orientation
        self.max_age = max_age
        self.view_filters = view_filters
        self.site_id = site_id
        self.blocking_refresh = blocking_refresh
        self.check_interval = check_interval
        self.tableau_conn_id = tableau_conn_id

    template_fields = [
        'output_path', 'view_filters'
    ]

    ui_color = '#ca8945'


    def execute(self, context: dict) -> None:
        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            server = tableau_hook.server
            workbook_id = self._get_workbook_id(tableau_hook)
            workbook = server.workbooks.get_by_id(workbook_id)
            server.workbooks.populate_views(workbook)
            views = workbook.views

            import tableauserverclient as TSC
            # Set pdf options
            pdf_req_options = TSC.PDFRequestOptions(
                page_type=self.page_type,
                orientation=self.orientation,
                maxage=self.max_age,
            )
            if self.view_filters is not None:
                for key, value in self.view_filters.items():
                    pdf_req_options.vf(key, value)


            tempdir = tempfile.mkdtemp(context['task_instance_key_str'])
            try:
                import functools
                download = functools.partial(
                    download_pdf, server, tempdir, pdf_req_options
                )
                downloaded = (download(view) for view in views)
                
                import PyPDF2
                merger = functools.reduce(
                    _combine_into,
                    downloaded,
                    PyPDF2.PdfFileMerger()
                )
                wb_url = get_wb_url(
                    tableau_conn=tableau_hook.get_connection(self.tableau_conn_id),
                    view=views[0],
                    view_filters=self.view_filters
                )
                if not '/Subject' in self.metadata.keys():
                    self.metadata['/Subject'] = f'{wb_url}'
                elif '/Keywords' not in self.metadata.keys():
                    self.metadata['/Keywords'] = f'url: {wb_url}'
                else:
                    self.metadata['/Keywords'] += f', url: {wb_url}'

                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if '/Keywords' not in self.metadata.keys():
                    self.metadata['/Keywords'] = f'last_updated: {ts}'
                else:
                    self.metadata['/Keywords'] += f', last_updated: {ts}'

                if self.metadata:
                    merger.addMetadata(infos=self.metadata)
                with open(self.output_path, 'wb') as f:
                    merger.write(f)
            finally:
                # Cleanup
                shutil.rmtree(tempdir)


    def _get_workbook_id(self, tableau_hook: TableauHook) -> str:

        if self.match_with == 'id':
            return self.find

        matches = [
            wb for wb in tableau_hook.get_all(resource_name='workbooks')\
            if getattr(wb, self.match_with) == self.find
        ]

        if len(matches) == 1:
            workbook = matches[0]
            workbook_id = workbook.id
            self.log.info('Found workbook matching with id %s', workbook_id)
            return workbook_id

        if len(matches) == 0:
            raise AirflowException(
                f'Workbook with {self.match_with} {self.find} not found!'
            )
        if len(matches) == 2:
            raise AirflowException(
                f'Found {len(matches)} workbooks with {self.match_with} '
                f'{self.find}! Please specify id. '
                f'Workbook ids: {[wb.id for wb in matches]}.'
            )
