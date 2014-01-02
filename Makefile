# vim: noet:
PREFIX:=../
DEST:=$(PREFIX)$(PROJECT)

REBAR=./rebar

DEPSOLVER_PLT=./.depsolver_plt

.PHONY: all edoc test clean build_plt dialyzer app

all:
	@$(REBAR) get-deps compile

edoc:
	@$(REBAR) doc

test:
	@rm -rf .eunit
	@mkdir -p .eunit
	@$(REBAR) skip_deps=true eunit

clean:
	@$(REBAR) clean

build_plt:
	@$(REBAR) build-plt

app:
	@$(REBAR) create template=mochiwebapp dest=$(DEST) appid=$(PROJECT)

$(DEPSOLVER_PLT):
	@dialyzer --output_plt $@ --build_plt \
		--apps erts kernel stdlib crypto ssh mnesia -r deps

dialyzer:$(DEPSOLVER_PLT)
	@dialyzer --plt $^ ./ebin/*.beam

typer:$(DEPSOLVER_PLT)
	@typer --plt $^ -r ./src -I ./include
