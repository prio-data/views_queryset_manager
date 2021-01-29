
# Queryset manager

This service is responsible for managing querysets, which are collections of
pointers to remote data.  These pointers are composed of operations (Ops) that
either transform data, or point to a raw data source. Ops are chained together
to form paths, that correspond to resources on the data router.

Querysets can be CRUDed, and are organized into Themes.
