# gcv_pangenomics

A pan-genomics extension module for the [Genome Context Viewer](https://github.com/legumeinfo/lis_context_viewer) (GCV).

Currently implements an optimal algorithm for a tractable variation of the Approximate Frequent Subpaths problem (AFS).

## Installation

To install the gcv_pangenomics extension on the GCV server, copy the `pangenomics` directory into the GCV `server` directory and add "gcv_pangenomics" to the list of installed apps in `settings.py`:

  INSTALLED_APPS = [
    ...
    'services',
    'pangenomics',
    ...
  ]

Next add the gcv_pangenomics urls to GCV server's `urls.py` file in `/server/server/`:

  urlpatterns = [
    ...
    url(r'^services/', include('services.urls')),
    url(r'^pangenomics/', include('pangenomics.urls')),
    ...
  ]

Lastly, add the gcv_pangenomics requirements to the GCV virtual environment:

  (venv) pip install -r pangenomics/requirements.txt

## Use

The AFS algorithm finds intervals on a query chromosome's ordered list of gene families such that other chromosomes in the database have similar gene family content as characterized by the GCV search query parameters - matched and intermediate - and a new parameter - support - which is the minimum number of a chromosomes that must have similar gene family content to an interval on the query chromosome in order for it to be considered frequent.

For now, such intervals can be found by directly visiting the following url:

  <host>/find-queries/<chromosome id>/<matched>/<intermediate>/<support>/
