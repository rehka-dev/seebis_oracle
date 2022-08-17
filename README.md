# Seebis Oracle
Simple abstraction layer for the SeeBIS instance aiming to add back pressure.
The current version is a POC and really buggy as well as bad written.
Although, the HTTP Pool is already up and running, however it fails if we request more data than available.
It should be rewritten and the task should take ownership over the CacheEntry, as well as place a CacheEntry::Running object, which will get replaced by an CacheEntry::Finished after successful download.

Next, the SQL should be also abstracted by leveraging the sqlx library.

