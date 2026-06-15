# Shared Library

Note that the shared library and shared infrastructure resources are two different things.

Currently, the shared library is mainly for common Pulumi Python constructs.

This is only required during development time and CI build time and mounted as a "hot reload" manner via [requirements-dev.txt](../requirements-dev.txt).

If you do `make install` or a direct `pip3 install -r requirements-dev.txt` once, that's all you need.
