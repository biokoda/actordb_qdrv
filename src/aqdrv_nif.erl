-module(aqdrv_nif).
-export([init/1, open/2, stage_map/4,stage_data/3,
	stage_flush/1, write/5,inject/4, set_tunnel_connector/0, set_thread_fd/4,
	replicate_opts/3,index_events/5, fsync/3, stop/0, init_tls/1]).

stop() ->
	exit(nif_library_not_loaded).
open(_,_) ->
	exit(nif_library_not_loaded).
stage_map(_,_,_,_) ->
	exit(nif_library_not_loaded).
stage_data(_,_,_) ->
	exit(nif_library_not_loaded).
stage_flush(_) ->
	exit(nif_library_not_loaded).
write(_,_,_,_,_) ->
	exit(nif_library_not_loaded).
inject(_,_,_,_) ->
	exit(nif_library_not_loaded).
index_events(_,_,_,_,_) ->
	exit(nif_library_not_loaded).
set_tunnel_connector() ->
	exit(nif_library_not_loaded).
set_thread_fd(_,_,_,_) ->
	exit(nif_library_not_loaded).
replicate_opts(_,_,_) ->
	exit(nif_library_not_loaded).
init_tls(_) ->
	exit(nif_library_not_loaded).
fsync(_,_,_) ->
	exit(nif_library_not_loaded).

init(Info) ->
	Schedulers = erlang:system_info(schedulers),
	LogName = "drv_"++hd(string:tokens(atom_to_list(node()),"@"))++".txt",
	NifName = "aqdrv_nif",
	NifFileName = case code:priv_dir(aqdrv) of
		{error, bad_name} -> filename:join("priv", NifName);
		Dir -> filename:join(Dir, NifName)
	end,
	case file:read_file_info(NifFileName) of
		{error,_} ->
			FN = filename:join("../priv/",NifName);
		_ ->
			FN = NifFileName
	end,
	case erlang:load_nif(FN, Info#{schedulers => Schedulers,logname => LogName}) of
		ok ->
			init_tls1();
		{error,{upgrade,_}} ->
			ok;
		{error,{reload,_}} ->
			ok
	end.

% Scheduler threads must know their index.
init_tls1() ->
	Sch = erlang:system_info(schedulers_online),
	[(catch spawn_opt(fun() -> init_tls(N) end, [{scheduler, N}])) || N <- lists:seq(1,Sch)],
	ok.
