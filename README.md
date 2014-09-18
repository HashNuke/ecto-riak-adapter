# Ecto.Adapters.Riak

Use Riak with Ecto, *just-like-that*.

> Write it like Riak, query it like Solr

### Features

* Writes model data as JSON
* Functions for Riak Search 2.0
* Namespaces all buckets for your app under a bucket type
* Works *just-like-that*

Project state: USELESS

** TODO: Add description **


### Notes

* Start Riak
* Set `search` option to `on` in `etc/riak.conf`
* Repo.create_search_schema("whatever")
* To index a bucket

  * either set the search index on the bucket type and set bucket type on a bucket

  ```
  riak-admin bucket-type create animals '{"props":{"search_index":"famous"}}'
  riak-admin bucket-type activate animals
  ```

  * set the search index on the bucket itself

### Search docs

* [TODO] Standard Query Parser - https://cwiki.apache.org/confluence/display/solr/The+Standard+Query+Parser
* Function queries - https://cwiki.apache.org/confluence/display/solr/Function+Queries
* Local paramters - https://cwiki.apache.org/confluence/display/solr/Local+Parameters+in+Queries
* More stuff - https://cwiki.apache.org/confluence/display/solr/Query+Syntax+and+Parsing
