test:
	nosetests -v

debug:
	export T_VERBOSE=9; nosetests --nocapture  --nologcapture -v

large:
	export T_VERBOSE=3; export T_LARGE=102; nosetests --nocapture  --nologcapture -v

case:
	nosetests --nocapture --nologcapture -v  unit.test_protocol:test_badreq

wait:
	export T_VERBOSE=9; nosetests --nocapture --nologcapture -v  unit.test_repl:repl_wait
