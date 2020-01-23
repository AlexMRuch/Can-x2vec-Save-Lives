# Can-x2vec-Save-Lives
Base code for Ruch (2020) "Can x2vec Save Lives? Integrating Graph and Language Embeddings for Automatic Mental Health Classification" - https://arxiv.org/abs/2001.01126.

*File information:*
- `SQLSampler_medium.sql` is used to draw a forest fire sample for a heterogeneous network from a postgres database with data from https://files.pushshift.io/reddit/
- `py4genMetaPaths.py` draws metapath-random walk samples from the heterogeneous network
- `doc2vec.ipynb` performs doc2vec embedding on subreddits, authors, and submissions in the network
- Code to run metapath2vec embedding models can be found at https://ericdongyx.github.io/metapath2vec/m2v.html
- `metapath2vec_medium.ipynb` uses the doc2vec and metapath2vec embedding models for visualization and classification tasks

Please note that in `metapath2vec_medium.ipynb` I import a `graph-tool` graph object to identify node attributes; however, you can create any kind of dictionary to store such information about nodes after drawing a sample with `SQLSampler_medium.sql`. Also, some printouts have been removed to prevent the display of potentially personally identifiable information.
