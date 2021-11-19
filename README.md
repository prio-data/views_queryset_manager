
# Queryset manager

This service is responsible for managing querysets, which are collections of
pointers to remote data.  These pointers are composed of operations (Ops) that
either transform data, or point to a raw data source. Ops are chained together
to form paths, that correspond to resources on the data router.

Querysets can be CRUDed, and are organized into Themes.

## Env settings

|Key                                                          |Description                    |Default                      |
|-------------------------------------------------------------|-------------------------------|-----------------------------|
|DB_HOST                                                      |                               |                             |
|DB_PORT                                                      |                               |                             |
|DB_USER                                                      |                               |                             |
|DB_NAME                                                      |                               |                             |
|LOG_LEVEL                                                    |                               |                             |
|JOB_MANAGER_URL                                              |                               |                             |

## Depends on 

* [views_job_manager](https://github.com/prio-data/views_job_manager)

## Contributing

For information about how to contribute, see [contributing](https://www.github.com/prio-data/contributing).
