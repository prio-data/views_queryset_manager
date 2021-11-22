
# Queryset manager

This service is responsible for managing querysets, which are collections of
pointers to remote data.  These pointers are composed of operations (Ops) that
either transform data, or point to a raw data source. Ops are chained together
to form paths, that correspond to resources on the data router.

Querysets can be CRUDed, and are organized into Themes.

## Env settings

|Key                                                          |Description                    |Default                      |
|-------------------------------------------------------------|-------------------------------|-----------------------------|
|DB_HOST                                                      |database hostname              |127.0.0.1                    |
|DB_PORT                                                      |port for database connnection. |5432                         |
|DB_USER                                                      |user for database connection.  |postgres                     |
|DB_NAME                                                      |dbname for database connection.|postgres                     |
|DB_PASSWORD                                                  |Optional password for database |None                         |
|DB_SSL                                                       |sslmode for database           |allow                        |
|LOG_LEVEL                                                    |Python logging level           |WARNING                      |
|JOB_MANAGER_URL                                              |URL for upstream data source   |http://job-manager           |

## Depends on 

* [views_job_manager](https://github.com/prio-data/views_job_manager)

## Contributing

For information about how to contribute, see [contributing](https://www.github.com/prio-data/contributing).
