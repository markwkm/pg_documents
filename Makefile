MODULE_big = pg_documents
OBJS = pg_documents.o
SHLIB_LINK += -ljson-c

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
