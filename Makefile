bundle:
	cd src &&\
	racetrack plugin bundle --out=..

install:
	racetrack plugin install *.zip
