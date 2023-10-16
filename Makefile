##
# Project Title
#
# @file
# @version 0.1

.PHONY:
install:
	@rm -rf certs || :
	@mkdir -p certs
	@cd certs && mkcert -install 0.0.0.0
