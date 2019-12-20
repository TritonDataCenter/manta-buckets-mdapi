#
# Copyright 2019 Joyent, Inc.
#

NAME = manta-buckets-mdapi

RUST_CODE = 1

SMF_MANIFESTS_IN = smf/manifests/buckets-mdapi.xml.in

ENGBLD_USE_BUILDIMAGE =	true
ENGBLD_REQUIRE := 	$(shell git submodule update --init deps/eng)

include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)


include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# Variables
#

# TODO: Use this to download or verify install of expected rust version
RUST_PREBUILT_VERSION =		1.33.0

RELEASE_TARBALL :=	$(NAME)-pkg-$(STAMP).tar.gz
ROOT :=			$(shell pwd)
RELSTAGEDIR :=		/tmp/$(NAME)-$(STAMP)

BASE_IMAGE_UUID   = cbf116a0-43a5-447c-ad8c-8fa57787351c
BUILDIMAGE_NAME   = mantav2-buckets-mdapi
BUILDIMAGE_DESC   = Manta buckets metadata API
AGENTS		  = amon config registrar

#
# Repo-specific targets
#
.PHONY: all
all: build-buckets-mdapi manta-scripts

.PHONY: manta-scripts
manta-scripts: deps/manta-scripts/.git
	mkdir -p $(BUILD)/scripts
	cp deps/manta-scripts/*.sh $(BUILD)/scripts

.PHONY: release
release: all deps docs $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/buckets-mdapi/deps
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/buckets-mdapi/bin
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot
	@mkdir -p $(RELSTAGEDIR)/site
	@touch $(RELSTAGEDIR)/site/.do-not-delete-me
	@mkdir -p $(RELSTAGEDIR)/root
	cp -r \
	    $(ROOT)/build \
	    $(ROOT)/sapi_manifests \
	    $(ROOT)/schema_templates \
	    $(ROOT)/migrations \
	    $(ROOT)/smf \
	    $(RELSTAGEDIR)/root/opt/smartdc/buckets-mdapi/
	cp target/release/buckets-mdapi $(RELSTAGEDIR)/root/opt/smartdc/buckets-mdapi/bin/
	cp target/release/schema-manager $(RELSTAGEDIR)/root/opt/smartdc/buckets-mdapi/bin/
	cp -r $(ROOT)/deps/manta-scripts \
	    $(RELSTAGEDIR)/root/opt/smartdc/buckets-mdapi/deps
	mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts
	cp -R $(RELSTAGEDIR)/root/opt/smartdc/buckets-mdapi/build/scripts/* \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts/
	cp -R $(ROOT)/boot/* \
	    $(RELSTAGEDIR)/root/opt/smartdc/boot/
	cd $(RELSTAGEDIR) && $(TAR) -I pigz -cf $(ROOT)/$(RELEASE_TARBALL) root site
	@rm -rf $(RELSTAGEDIR)

#
# We include a pre-substituted copy of the template in our built image so that
# registrar doesn't go into maintenance on first boot. Then we ship both the
# .in file and this "bootstrap" substituted version. The boot/setup.sh script
# will perform this replacement again during the first boot, replacing @@PORTS@@
# with the real port.
#
# This default value should be kept in sync with the default value in
# boot/setup.sh and sapi_manifests/buckets-mdapi/template
#
#
sapi_manifests/registrar/template: sapi_manifests/registrar/template.in
	sed -e 's/@@PORTS@@/2030/g' $< > $@

.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(ROOT)/$(RELEASE_TARBALL) \
	    $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

.PHONY: build-buckets-mdapi
build-buckets-mdapi:
	$(CARGO) build --release

.PHONY: test-unit
test-unit:
	$(CARGO) test --lib

include ./deps/eng/tools/mk/Makefile.deps
include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
include ./deps/eng/tools/mk/Makefile.smf.targ
include ./deps/eng/tools/mk/Makefile.targ
