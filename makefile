SRC_PATH=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

clean:
	rm -rf\
	 $(SRC_PATH)/dist\
	 $(SRC_PATH)/debug\
	 $(SRC_PATH)/godepgraph.png\
	 $(SRC_PATH)/*/cover.out\
	 $(SRC_PATH)/*/cover.html

bigquery_launch:
	printf "Y\n" | gcloud components install bigquery-emulator