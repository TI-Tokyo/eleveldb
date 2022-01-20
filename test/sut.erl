-module(sut).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%%=======================================================================
%% This is all stuff we need to mock up msgpack-encoding of riak objects
%%=======================================================================

-define(V1_VERS, 1).
-define(MAGIC, 53).
-define(MSGPACK_MAGIC, 2). %% Magic number for msgpack encoding                                  
-define(EMPTY_VTAG_BIN, <<"e">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_DELETED,  <<"X-Riak-Deleted">>).
-define(MD_CTYPE,    <<"content-type">>).
-define(MAX_KEY_SIZE, 65536).
-define(DAYS_FROM_GREGORIAN_BASE_TO_EPOCH, (1970*365+478)).
-define(SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH,
        (?DAYS_FROM_GREGORIAN_BASE_TO_EPOCH * 24*60*60)
       ).

-export_type([vclock/0, timestamp/0, vclock_node/0, dot/0, pure_dot/0]).
-export_type([riak_object/0, bucket/0, key/0, value/0, binary_version/0]).

-type vclock() :: [dot()].
-type dot() :: {vclock_node(), {counter(), timestamp()}}.
-type pure_dot() :: {vclock_node(), counter()}.

% Nodes can have any term() as a name, but they must differ from each other.

-type   vclock_node() :: term().
-type   counter() :: integer().
-type   timestamp() :: integer().

% @doc Create a brand new vclock.
-spec fresh() -> vclock().
fresh() ->
    [].

-spec fresh(vclock_node(), counter()) -> vclock().
fresh(Node, Count) ->
    [{Node, {Count, timestamp()}}].

% @doc Return a timestamp for a vector clock
-spec timestamp() -> timestamp().
timestamp() ->
    %% Same as calendar:datetime_to_gregorian_seconds(erlang:universaltime()),
    %% but significantly faster.
    {MegaSeconds, Seconds, _} = os:timestamp(),
        ?SECONDS_FROM_GREGORIAN_BASE_TO_EPOCH + MegaSeconds*1000000 + Seconds.

-type key() :: binary().
-type bucket() :: binary() | {binary(), binary()}.
-type value() :: term().

-record(r_content, {
          metadata :: dict() | list(),
          value :: term()
         }).

-record(r_object, {
          bucket :: bucket(),
          key :: key(),
          contents :: [#r_content{}],
          vclock = fresh() :: vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).

-opaque riak_object() :: #r_object{}.

%% @doc Constructor for new riak objects.
-spec new(Bucket::bucket(), Key::key(), Value::value()) -> riak_object().
new({T, B}, K, V) when is_binary(T), is_binary(B), is_binary(K) ->
    new_int({T, B}, K, V, no_initial_metadata);
new(B, K, V) when is_binary(B), is_binary(K) ->
    new_int(B, K, V, no_initial_metadata).

%% @doc Constructor for new riak objects with an initial content-type.
-spec new(Bucket::bucket(), Key::key(), Value::value(),
          string() | dict() | no_initial_metadata) -> riak_object().
new({T, B}, K, V, C) when is_binary(T), is_binary(B), is_binary(K), is_list(C) ->
    new_int({T, B}, K, V, dict:from_list([{?MD_CTYPE, C}]));
new(B, K, V, C) when is_binary(B), is_binary(K), is_list(C) ->
    new_int(B, K, V, dict:from_list([{?MD_CTYPE, C}]));

%% @doc Constructor for new riak objects with an initial metadata dict.
%%
%% NOTE: Removed "is_tuple(MD)" guard to make Dialyzer happy.  The previous clause
%%       has a guard for string(), so this clause is OK without the guard.
new({T, B}, K, V, MD) when is_binary(T), is_binary(B), is_binary(K) ->
    new_int({T, B}, K, V, MD);
new(B, K, V, MD) when is_binary(B), is_binary(K) ->
    new_int(B, K, V, MD).

%% internal version after all validation has been done
new_int(B, K, V, MD) ->
    case size(K) > ?MAX_KEY_SIZE of
        true ->
            throw({error,key_too_large});
        false ->
            case MD of
                no_initial_metadata ->
                    Contents = [#r_content{metadata=dict:new(), value=V}],
                    #r_object{bucket=B,key=K,
                              contents=Contents,vclock=fresh()};
                _ ->
                    Contents = [#r_content{metadata=MD, value=V}],
                    #r_object{bucket=B,key=K,updatemetadata=MD,
                              contents=Contents,vclock=fresh()}
            end
    end.

is_robject(#r_object{}) ->
    true;
is_robject(_) ->
    false.

-spec get_contents(riak_object()) -> [{dict(), term()}].
get_contents(#r_object{contents=Contents}) ->
    [{Content#r_content.metadata, Content#r_content.value} ||
        Content <- Contents].

-spec bucket(riak_object()) -> bucket().
bucket(Obj1) ->
    Obj1#r_object.bucket.

-spec contents(riak_object()) -> term().
contents(Obj1) ->
    Obj1#r_object.contents#r_content.value.


-type binary_version() :: v0 | v1.
-type encoding() :: erlang | msgpack.


-spec to_binary(binary_version(), riak_object()) -> binary().
-spec to_binary(binary_version(), riak_object(), encoding()) -> binary().

to_binary(Vers, Robj) ->
    to_binary(Vers, Robj, erlang).
to_binary(v0, RObj, _) ->
    term_to_binary(RObj);
to_binary(v1, #r_object{contents=Contents, vclock=VClock}, Enc) ->
    new_v1(VClock, Contents, Enc).

new_v1(Vclock, Siblings, Enc) ->
    VclockBin = term_to_binary(Vclock),
    VclockLen = byte_size(VclockBin),
    SibCount = length(Siblings),
    SibsBin = bin_contents(Siblings, Enc),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer, VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

bin_content(#r_content{metadata=Meta, value=Val}, Enc) ->
    ValBin = encode(Val, Enc),
    ValLen = byte_size(ValBin),
    MetaBin = meta_bin(Meta),
    MetaLen = byte_size(MetaBin),
    <<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary>>.

bin_contents(Contents, Enc) ->
    F = fun(Content, Acc) ->
                <<Acc/binary, (bin_content(Content, Enc))/binary>>
        end,
    lists:foldl(F, <<>>, Contents).

meta_bin(MD) ->
    {{VTagVal, Deleted, LastModVal}, RestBin} = dict:fold(fun fold_meta_to_bin/3,
                                                          {{undefined, <<0>>, undefined}, <<>>},
                                                          MD),
    VTagBin = case VTagVal of
                  undefined ->  ?EMPTY_VTAG_BIN;
                  _ -> list_to_binary(VTagVal)
              end,
    VTagLen = byte_size(VTagBin),
    LastModBin = case LastModVal of
                     undefined -> <<0:32/integer, 0:32/integer, 0:32/integer>>;
                     {Mega,Secs,Micro} -> <<Mega:32/integer, Secs:32/integer, Micro:32/integer>>
                 end,
    <<LastModBin/binary, VTagLen:8/integer, VTagBin:VTagLen/binary,
      Deleted:1/binary-unit:8, RestBin/binary>>.

fold_meta_to_bin(?MD_VTAG, Value, {{_Vt,Del,Lm},RestBin}) ->
    {{Value, Del, Lm}, RestBin};
fold_meta_to_bin(?MD_LASTMOD, Value, {{Vt,Del,_Lm},RestBin}) ->
     {{Vt, Del, Value}, RestBin};
fold_meta_to_bin(?MD_DELETED, true, {{Vt,_Del,Lm},RestBin})->
     {{Vt, <<1>>, Lm}, RestBin};
fold_meta_to_bin(?MD_DELETED, "true", Acc) ->
    fold_meta_to_bin(?MD_DELETED, true, Acc);
fold_meta_to_bin(?MD_DELETED, _, {{Vt,_Del,Lm},RestBin}) ->
    {{Vt, <<0>>, Lm}, RestBin};
fold_meta_to_bin(Key, Value, {{_Vt,_Del,_Lm}=Elems,RestBin}) ->
    ValueBin = encode_maybe_binary(Value),
    ValueLen = byte_size(ValueBin),
    KeyBin = encode_maybe_binary(Key),
    KeyLen = byte_size(KeyBin),
    MetaBin = <<KeyLen:32/integer, KeyBin/binary, ValueLen:32/integer, ValueBin/binary>>,
    {Elems, <<RestBin/binary, MetaBin/binary>>}.

encode(Bin, Enc) when Enc == erlang ->
    encode_maybe_binary(Bin);
encode(Bin, Enc) when Enc == msgpack ->
    encode_msgpack(Bin).

encode_msgpack(Bin) ->    
    <<?MSGPACK_MAGIC:8/integer, (msgpack:pack(Bin, [{format, jsx}]))/binary>>.

em(Bin) ->
    MsgBin = msgpack:pack(Bin, [{format, jsx}]),
    MsgBin.

encode_maybe_binary(Bin) when is_binary(Bin) ->
    <<1, Bin/binary>>;
encode_maybe_binary(Bin) ->    
    <<0, (term_to_binary(Bin))/binary>>.


%% @doc return the binary version the riak object binary is encoded in
-spec binary_version(binary()) -> binary_version().
binary_version(<<131,_/binary>>) -> v0;
binary_version(<<?MAGIC:8/integer, 1:8/integer, _/binary>>) -> v1.

%% @doc Convert binary object to riak object
-spec from_binary(bucket(),key(),binary()) ->
    riak_object() | {error, 'bad_object_format'}.
from_binary(B,K,Obj) ->
    from_binary(B,K,Obj,erlang).

-spec from_binary(bucket(),key(),binary(), encoding()) ->
    riak_object() | {error, 'bad_object_format'}.
from_binary(_B,_K,<<131, _Rest/binary>>=ObjTerm, _) ->
    binary_to_term(ObjTerm);
from_binary(B,K,<<?MAGIC:8/integer, 1:8/integer, Rest/binary>>=_ObjBin, Enc) ->
    %% Version 1 of binary riak object
    case Rest of
        <<VclockLen:32/integer, VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> ->
            Vclock = binary_to_term(VclockBin),
            Contents = sibs_of_binary(SibCount, SibsBin, Enc),
            #r_object{bucket=B,key=K,contents=Contents,vclock=Vclock};
        _Other ->
            {error, bad_object_format}
    end;
from_binary(_B, _K, Obj = #r_object{}, _) ->
    Obj.

sibs_of_binary(Count,SibsBin, Enc) ->
    sibs_of_binary(Count, SibsBin, [], Enc).

sibs_of_binary(0, <<>>, Result, _) -> lists:reverse(Result);
sibs_of_binary(0, _NotEmpty, _Result, _) ->
    {error, corrupt_contents};
sibs_of_binary(Count, SibsBin, Result, Enc) ->
    {Sib, SibsRest} = sib_of_binary(SibsBin, Enc),
    sibs_of_binary(Count-1, SibsRest, [Sib | Result], Enc).

sib_of_binary(<<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary, Rest/binary>>, _Enc) ->
    <<LMMega:32/integer, LMSecs:32/integer, LMMicro:32/integer, VTagLen:8/integer, VTag:VTagLen/binary, Deleted:1/binary-unit:8, MetaRestBin/binary>> = MetaBin,

    MDList0 = deleted_meta(Deleted, []),
    MDList1 = last_mod_meta({LMMega, LMSecs, LMMicro}, MDList0),
    MDList2 = vtag_meta(VTag, MDList1),
    MDList = meta_of_binary(MetaRestBin, MDList2),
    MD = dict:from_list(MDList),
    {#r_content{metadata=MD, value=decode_maybe_binary(ValBin)}, Rest}.

deleted_meta(<<1>>, MDList) ->
    [{?MD_DELETED, "true"} | MDList];
deleted_meta(_, MDList) ->
    MDList.

last_mod_meta({0, 0, 0}, MDList) ->
    MDList;
last_mod_meta(LM, MDList) ->
    [{?MD_LASTMOD, LM} | MDList].

vtag_meta(?EMPTY_VTAG_BIN, MDList) ->
    MDList;
vtag_meta(VTag, MDList) ->
    [{?MD_VTAG, binary_to_list(VTag)} | MDList].

meta_of_binary(<<>>, Acc) ->
    Acc;
meta_of_binary(<<KeyLen:32/integer, KeyBin:KeyLen/binary, ValueLen:32/integer, ValueBin:ValueLen/binary, Rest/binary>>, ResultList) ->
    Key = decode_maybe_binary(KeyBin),
    Value = decode_maybe_binary(ValueBin),
    meta_of_binary(Rest, [{Key, Value} | ResultList]).

decode_msgpack(ValBin) ->
    {ok, Unpacked} = msgpack:unpack(ValBin, [{format, jsx}]),
    Unpacked.

decode_maybe_binary(<<1, Bin/binary>>) ->
    Bin;
decode_maybe_binary(<<0, Bin/binary>>) ->
    binary_to_term(Bin);
decode_maybe_binary(<<?MSGPACK_MAGIC:8/integer, Bin/binary>>) ->
    decode_msgpack(Bin).

%%=======================================================================
%% Generic utilities we need to run tests
%%=======================================================================

open() ->
    {ok, Ref} = eleveldb:open("ltest", [{create_if_missing, true}]),
    Ref.

open(Table) ->
    {ok, Ref} = eleveldb:open(Table, [{create_if_missing, true}]),
    Ref.

close(Ref) ->
    eleveldb:close(Ref).

clearDb() ->
    os:cmd("rm -rf ltest").

myformat(T) ->
    io:format("\r"),
    io:format(T),
    io:nl().

myformat(T,Val) ->
    io:format("\r"),
    io:format(T,[Val]),
    io:nl().

myformat(T,Val1, Val2) ->
    io:format("\r"),
    io:format(T,[Val1, Val2]),
    io:nl().

%%=======================================================================
%% Utilities needed for streaming fold tests
%%=======================================================================

%%------------------------------------------------------------
%% Recursive function to put keys in the database
%%------------------------------------------------------------

putKeysObj(N) -> 
    putKeysObj(N, msgpack).

putKeysObj(N, Enc) -> 
    clearDb(),
    putKeysObj(open(), N, Enc).
putKeysObj(Ref, N, Enc) -> 
    putKeysObj(Ref, N, 1, Enc).
putKeysObj(Ref, N, Acc, Enc) when Acc =< N -> 
    ValList = [{<<"f1">>, Acc},  {<<"f2">>, <<"test2">>}, {<<"f3">>, 3}, {<<"f4">>, [1,2,3]}, {<<"f5">>, false}],
    addKey(Ref, ValList, Acc, N, Enc);
putKeysObj(Ref, N, Acc, _Enc) when Acc > N ->
    close(Ref).

%%------------------------------------------------------------
%% Iterative function add a key to the backend
%%------------------------------------------------------------

%% If Enc = mixed, alternate encodings between msgpack and erlang

addKey(Ref, ValList, Acc, N, mixed) ->
    Ndig = trunc(math:log10(N)) + 1,
    Key  = list_to_binary("key" ++ string:right(integer_to_list(Acc), Ndig, $0)),
    Obj  = new(<<"bucket">>, Key, ValList),
    Enc = 
        case Acc rem 2 of
            0 ->
                msgpack;
            _ ->
                erlang
        end,
    Val  = to_binary(v1, Obj, Enc),
    ok   = eleveldb:put(Ref, Key, Val, []),
    putKeysObj(Ref, N, Acc+1, mixed);

%% Else just use the specified encoding

addKey(Ref, ValList, Acc, N, Enc) ->
    Ndig = trunc(math:log10(N)) + 1,
    Key  = list_to_binary("key" ++ string:right(integer_to_list(Acc), Ndig, $0)),
    Obj  = new(<<"bucket">>, Key, ValList),
    Val  = to_binary(v1, Obj, Enc),
    ok   = eleveldb:put(Ref, Key, Val, []),
    putKeysObj(Ref, N, Acc+1, Enc).

%%------------------------------------------------------------
%% Explicit function to add a key to the backend
%%------------------------------------------------------------

addKey(Ref, KeyNum, ValList) ->
    addKey(Ref, KeyNum, ValList, msgpack).

addKey(Ref, ValList, Acc, N) when is_integer(N) ->
    addKey(Ref, ValList, Acc, N, msgpack);

addKey(Ref, KeyNum, ValList, Enc) ->
    Key = list_to_binary("key"++integer_to_list(KeyNum)),
    Obj = new(<<"bucket">>, Key, ValList),
    Val = to_binary(v1, Obj, Enc),
    ok = eleveldb:put(Ref, Key, Val, []).

%%------------------------------------------------------------
%% Get the value (contents) of a msgpack-encoded riak object
%%------------------------------------------------------------

getKeyVal(K,V) ->
    Obj = from_binary(<<"bucket">>, K, V, msgpack),
    [{_,Contents}] = get_contents(Obj),
    Contents.

getKeyVal(B,K,V) ->
    Obj = from_binary(B, K, V, msgpack),
    [{_,Contents}] = get_contents(Obj),
    Contents.

%%------------------------------------------------------------
%% Fold over keys using streaming folds.
%%
%% Now that eleveldb behavior has been changed to throw errors on
%% unsupported operators, we trap them and return an empty list
%%
%%------------------------------------------------------------

streamFoldTest(Filter, PutKeyFun, []) ->
    clearDb(),
    Opts=[{fold_method, streaming},
          {range_filter, Filter}],
    Ref = open(),

    PutKeyFun(Ref),

% Build a list of returned keys

    FF = fun({K,V}, Acc) -> 
                 [getKeyVal(K,V) | Acc]
         end,

    Keys = 
        try 
            Acc = eleveldb:fold(Ref, FF, [], Opts),
            ok = eleveldb:close(Ref),
            lists:reverse(Acc)
        catch
            error:_Error ->

		%% This is used for deliberate failure testing, and
		%% the 'correct' behavior is indicated by return of an
		%% empty list [].  We fail quietly, so that we don't
		%% print alarming messages during unit testing
		%%
		%%io:format(user, "Caught an error: closing db~n", []),

                ok = eleveldb:close(Ref),
                []
        end,
    Keys.

get_field(Field, List) ->
    lists:keyfind(Field, 1, List).

match([], _, _, _) ->
    0;
match(List, Field, CompVal, CompFun) ->
    {_,Val} = get_field(Field, List),
    match(Val,CompVal,CompFun).

match(V1,{CompVal},CompFun) ->
    match(V1, CompVal, CompFun);
match(V1,{_FilterVal, CompVal},CompFun) ->
    match(V1, CompVal, CompFun);
%% Match of empty list against empty list is valid
match([],[],_CompFun) ->
    1;
%% Match of empty list against anything else is not
match([],_V2,_CompFun) ->
    0;
match(V1,V2,CompFun) ->

    case CompFun(V1,V2) of
        true -> 
%%	    io:format("Match: comparing ~p to ~p returning true~n", [V1, V2]),
	    1;
        _ -> 
%%	    io:format("Match: comparing ~p to ~p returning false~n", [V1, V2]),
	    0
    end.

fieldsMatching(Vals, Field, CompVal, CompFun) ->
    lists:foldl(fun(Val, {N, Nmatch}) -> 
                        {N + 1, Nmatch + match(Val, Field, CompVal, CompFun)} end, {0,0}, Vals).

%%=======================================================================
%% Actual tests begin here
%%=======================================================================

%%=======================================================================
%% Test that we can pack and unpack erlang/msgpack-encoded objects
%%=======================================================================

packObj_test() ->
    io:format("packObj_test~n"),
    Obj = new(<<"bucket">>, <<"key">>, [{<<"field1">>, 1}, {<<"field2">>, 2.123}]),
    PackedErl = to_binary(v1, Obj, erlang),
    PackedMsg = to_binary(v1, Obj, msgpack),
    ObjErl = from_binary(<<"bucket">>, <<"key">>, PackedErl, erlang),
    ObjMsg = from_binary(<<"bucket">>, <<"key">>, PackedMsg, msgpack),
    Res = (ObjErl == Obj) and (ObjMsg == Obj),
    ?assert(Res),
    Res.

%%=======================================================================
%% Operations tests
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for operations tests
%%-----------------------------------------------------------------------

putKeyNormalOpsMsgpack(Ref) ->
    putKeyNormalOps(Ref, msgpack).

putKeyNormalOpsErlang(Ref) ->
    putKeyNormalOps(Ref, erlang).

putKeyNormalOpsMixed(Ref) ->
    putKeyNormalOps(Ref, mixed).

putKeyNormalOps(Ref, mixed) ->
    Rows = getKeyNormalOps(),
    lists:foreach(fun (Row) ->
                          I = element(2, hd(Row)),
			  Enc = 
			      case I rem 2 of
				  0 ->
				      msgpack;
				  _ ->
				      erlang
			      end,
                          addKey(Ref, I, Row, Enc)
                  end, Rows);
putKeyNormalOps(Ref, Enc) ->
    Rows = getKeyNormalOps(),
    lists:foreach(fun (Row) ->
                          I = element(2, hd(Row)),
                          addKey(Ref, I, Row, Enc)
                  end, Rows).

%------------------------------------------------------------
% Return a list of keys used for testing
%------------------------------------------------------------

getKeyNormalOps() ->
    FieldCount = 8,

    %% A function for constructing integer fields of variable type

    VarIntFn = 
        fun(I) ->
                Prefactor = 
                    case I rem 2 of
                        0 -> 
                            1;
                        _ ->
                            -1
                    end,
                AddFactor = 
                    case (I-1) div 2 of
                        0 ->
                            0;
                        1 ->
                            256;
                        2 ->
                            65536;
                        3 ->
                            4294967296;
                        _ ->
                            1099511627776
                    end,
                Prefactor * (I + AddFactor)
        end,

    %% A function for constructing arbitrary-type fields as either
    %% NULL or not-NULL (by use of supplied CF fun)

    CreateFn = 
	fun(I, V1, V2, UseNull, CF) -> 
		case UseNull of
		    true ->
			case I of
			    V1 -> [];
			    V2 -> [];
			    _ -> CF(I)
			end;
		    _ ->
			CF(I)
		end
	end,

    UseNull = true,
    F2 = 2*FieldCount,

    Rows = [ [{<<"f1">>, I},
              {<<"f2">>, 
	       CreateFn(I, 4, F2, UseNull, fun(Ind) -> list_to_binary("test" ++ integer_to_list(Ind)) end)
	      }, %%<< varchar
              {<<"f3">>, 
	       CreateFn(I, 6, F2, UseNull, fun(Ind) -> Ind * 1.0 end)
	      }, %%<< double
              {<<"f4">>, 
	       CreateFn(I, 8, F2, UseNull, fun(Ind) -> Ind rem 2 =:= 0 end)
	      }, %%<< boolean
	      {<<"f5">>, 
	       CreateFn(I, 8, F2, UseNull, fun(Ind) -> lists:seq(Ind, Ind + 2) end)
	      }, %%<< list (not a TS type)
	      {<<"f6">>, 
	       CreateFn(I, 10, F2, UseNull, fun(Ind) -> Ind * 1000 end)
	      }, %%<< sint64
	      {<<"f7">>, 
	       CreateFn(I, 12, F2, UseNull, fun(Ind) -> 1467563367600 + Ind * 1000 end)
	      }, %%<< sint64 stored as a big num
	      {<<"f8">>, 
	       CreateFn(I, 12, F2, UseNull, fun(Ind) -> VarIntFn(Ind) end)
	      }  %%<< sint64 stored as a variety of types
             ] || I <- lists:seq(1, FieldCount * 2) ],  %%<< twice to store a NULL value for each field type
    Rows.

%------------------------------------------------------------
% Return the appropriate put fn for whichever encoding we want to test
%------------------------------------------------------------

getPutFn(msgpack) ->
    fun putKeyNormalOpsMsgpack/1;
getPutFn(erlang) ->
    fun putKeyNormalOpsErlang/1;
getPutFn(mixed) ->
    fun putKeyNormalOpsMixed/1.

%------------------------------------------------------------
% Put realistic data
%------------------------------------------------------------

putKeyRealisticOpsMsgpack(Ref) ->
    putSequentialRealisticData({Ref, msgpack, 1467554400000, 1467563400000, 0.5}, 20, 0).

putKeyRealisticOpsErlang(Ref) ->
    putSequentialRealisticData({Ref, erlang,  1467554400000, 1467563400000, 0.5}, 20, 0).

putSequentialRealisticData(_Args, _Nrow, _Nrow) ->
    ok;
putSequentialRealisticData(Args, Nrow, AccRow) ->
    {Ref, Enc, StartTime, Delta, LapsFrac} = Args,
    Data = getRealisticData(<<"596044d8-86f5-462f-8d94-65b25e7d3fe9">>, StartTime + AccRow * Delta, LapsFrac),

%%    case AccRow rem 10 of 
%%      0 ->
%%          io:format("Putting row ~p Data = ~p~n", [AccRow, Data]);
%%      _ ->
%%          ok
%%    end,

    addKey(Ref, AccRow, Data, Enc),
    putSequentialRealisticData(Args, Nrow, AccRow+1).

getRealisticData(SportEventUuid, Timestamp, LapsFrac) ->
    VarcharSize = length(binary_to_list(SportEventUuid)),

    LapsRand = random:uniform(1000),
    LapsComp = LapsFrac * 1000,
    Laps = 
        case LapsRand =< LapsComp of
            true ->
                10;
            false ->
                -1
        end,
    [{<<"sport_event_uuid">>,        SportEventUuid}, 
     {<<"time">>,                    Timestamp}, 
     {<<"club_uuid">>,               list_to_binary(get_random_string(VarcharSize))},
     {<<"person_uuid">>,                     list_to_binary(get_random_string(VarcharSize))},
     {<<"sport_uuid">>,              list_to_binary(get_random_string(VarcharSize))},
     {<<"discipline_uuid">>,         list_to_binary(get_random_string(VarcharSize))},
     {<<"driver_number">>,           list_to_binary(get_random_string(VarcharSize))},
     {<<"person_full_name">>,        list_to_binary(get_random_string(VarcharSize))},
     {<<"club_full_name">>,          list_to_binary(get_random_string(VarcharSize))},
     {<<"abandoned">>,               false},
     {<<"best_gap_in_time">>,        float(random:uniform(100))},
     {<<"best_gap_in_lap">>,         random:uniform(100)},
     {<<"best_lap">>,                float(random:uniform(100))},
     {<<"best_position">>,           random:uniform(100)},
     {<<"best_sector_1">>,           float(random:uniform(100))},
     {<<"best_sector_2">>,           float(random:uniform(100))},
     {<<"best_sector_3">>,           float(random:uniform(100))},
     {<<"best_speed">>,              float(random:uniform(100))},
     {<<"gap_in_time">>,                     float(random:uniform(100))},
     {<<"gap_in_lap">>,              random:uniform(100)},
     {<<"lap_timel">>,               float(random:uniform(100))},
     {<<"laps">>,                    Laps},
     {<<"position">>,                random:uniform(100)},
     {<<"qualification_position">>,   random:uniform(100)},
     {<<"sector_1">>,                float(random:uniform(100))},
     {<<"sector_2">>,                float(random:uniform(100))},
     {<<"sector_3">>,                float(random:uniform(100))},
     {<<"info">>,                    list_to_binary(get_random_string(VarcharSize))}].

get_random_string(Length) ->
    AllowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
    lists:foldl(fun(_, Acc) ->
                        [lists:nth(random:uniform(length(AllowedChars)),
                                   AllowedChars)]
                            ++ Acc
                end, [], lists:seq(1, Length)).

%------------------------------------------------------------
% Default evaluation function checks that the total number of keys
% returned from a filter is greater than zero and equals the expected
% number of keys
%------------------------------------------------------------

defaultEvalFn({Val, NullMatch=[]}) ->
    ?assertEqual(Val, NullMatch);
defaultEvalFn({Null=[], NullMatch}) ->
    ?assertEqual(Null, NullMatch);
defaultEvalFn({N, Nmatch}) ->
    (N > 0) and (N == Nmatch).

filterVal({FilterVal}) ->
    FilterVal;
filterVal({FilterVal, _CompVal}) ->
    FilterVal.

%------------------------------------------------------------
% Test each operation against the specified Field and Val
%------------------------------------------------------------

isNullOpsFactory(IsNull, Field, PutFn, EvalFn) ->
    %% for NULL equality check, cast to binary is needed, the alternative of
    %% creating an is_null and is_not_null expression w/i eleveldb is considered
    %% but unless performance suffers, a binary comparison to reuse equality
    %% and nonEquality comparitors is preferable.
    Type = varchar,
    Val = [],
    Op = case IsNull of
        true -> '=';
        _ -> '!='
    end,
    Filter = {Op, {field, Field, Type}, {const, Val}},
    Keys = streamFoldTest(Filter, PutFn, []),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 == V2 end)).

isNullOps({Field, _Val, _Type, PutFn, EvalFn}) ->
    isNullOpsFactory(true, Field, PutFn, EvalFn).

isNotNullOps({Field, _Val, _Type, PutFn, EvalFn}) ->
    isNullOpsFactory(false, Field, PutFn, EvalFn).

eqOps({Field, Val, Type, PutFn, EvalFn, OtherArgs}) ->
    Filter = {'=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn, OtherArgs),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 == V2 end)).
    
eqEqOps({Field, Val, Type, PutFn, EvalFn, OtherArgs}) ->
    Filter = {'==', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn, OtherArgs),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 == V2 end)).

neqOps({Field, Val, Type, PutFn, EvalFn, OtherArgs}) ->
    Filter = {'!=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn, OtherArgs),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 /= V2 end)).

gtOps({Field, Val, Type, PutFn, EvalFn, OtherArgs}) ->
    Filter = {'>', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn, OtherArgs),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 > V2 end)).

gteOps({Field, Val, Type, PutFn, EvalFn, OtherArgs}) ->
    Filter = {'>=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn, OtherArgs),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 >= V2 end)).

ltOps({Field, Val, Type, PutFn, EvalFn, OtherArgs}) ->
    Filter = {'<', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn, OtherArgs),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 < V2 end)).

lteOps({Field, Val, Type, PutFn, EvalFn, OtherArgs}) ->
    Filter = {'=<', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn, OtherArgs),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 =< V2 end)).

%------------------------------------------------------------
% All of the above operation tests
%------------------------------------------------------------

allOps(Args) ->
    eqOps(Args) and eqEqOps(Args) and neqOps(Args) and 
        gtOps(Args) and gteOps(Args) and
        ltOps(Args) and lteOps(Args).

%------------------------------------------------------------
% Only equality operations
%------------------------------------------------------------

eqOpsOnly(Args) ->
    eqOps(Args) and eqEqOps(Args) and neqOps(Args).

%------------------------------------------------------------
% All comparison operations
%------------------------------------------------------------

allCompOps(Args) ->
    gtOps(Args) and gteOps(Args) and
        ltOps(Args) and lteOps(Args).

%------------------------------------------------------------
% Any comparison operation
%------------------------------------------------------------

anyCompOps(Args) ->
    gtOps(Args) or gteOps(Args) or
        ltOps(Args) or lteOps(Args).

%%=======================================================================
%% Data-type specific tests
%%=======================================================================

%%------------------------------------------------------------
%% Test timestamp operations
%%------------------------------------------------------------

timestampOps_test() ->
    {timeout, 60, fun() -> timestampOpsTests() end}.
			  
timestampOpsTests() ->
    timestampOps(erlang) and timestampOps(msgpack) and timestampOps(mixed).

timestampOps(Enc) ->
    io:format("timestampOps_test with ~p encoding~n", [Enc]),
    F = <<"f6">>,
    Val = 2000,
    PutFn = getPutFn(Enc),
    EvalFn = fun defaultEvalFn/1,

    %% Timestamps support all ops

    Res = allOps({F, {Val}, timestamp, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test sint64 operations, because now that's what we're calling integers
%%------------------------------------------------------------

sint64Ops_test() ->
    {timeout, 60, fun() -> sint64OpsTests() end}.

sint64OpsTests() ->
    sint64Ops(msgpack) and sint64Ops(erlang) and sint64Ops(mixed).

sint64Ops(Enc) ->
    io:format("sint64Ops_test with ~p encoding~n", [Enc]),
    F = <<"f1">>,
    Val = 3,
    PutFn = getPutFn(Enc),
    EvalFn = fun defaultEvalFn/1,

    %% sint64s support all ops

    Res = allOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test variable-type integer operations (both msgpack and ttb encode
%% integers differently according to value, field f8 is specifically
%% designed to probe all possible cases)
%%------------------------------------------------------------

varIntOps_test() ->
    {timeout, 60, fun() -> varIntOpsTests() end}.

varIntOpsTests() ->
    varIntOps(msgpack) and varIntOps(erlang) and varIntOps(erlang).

varIntOps(Enc) ->
    io:format("varIntOps_test with ~p encoding~n", [Enc]),
    F = <<"f8">>,
    Val = 2,
    PutFn = getPutFn(Enc),
    EvalFn = fun defaultEvalFn/1,

    %% variable integers support all ops

    Res = allOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test varchar operations, because that's what were now calling
%% binary
%%------------------------------------------------------------

varcharOps_test() ->
   {timeout, 60, fun() -> varcharOpsTests() end}.
			 
varcharOpsTests() ->
    varcharOps(msgpack) and varcharOps(erlang) and varcharOps(mixed).

varcharOps(Enc) ->
    io:format("varcharOps_test with ~p encoding~n", [Enc]),
    F = <<"f2">>,
    Val = <<"test3">>,
    PutFn = getPutFn(Enc),
    EvalFn = fun defaultEvalFn/1,

    %% varchar types support only equality operations

    Res = eqOpsOnly({F, {Val}, varchar, PutFn, EvalFn, []}) and (anyCompOps({F, {Val}, varchar, PutFn, EvalFn, []}) == false),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test double operations, because that's what we're now calling
%% 'float'
%%------------------------------------------------------------

doubleOps_test() ->
    {timeout, 60, fun() -> doubleOpsTests() end}.

doubleOpsTests() ->
    doubleOps(msgpack) and doubleOps(erlang) and doubleOps(mixed).

doubleOps(Enc) ->
    io:format("doubleOps_test with ~p encoding~n", [Enc]),
    F = <<"f3">>,
    Val = 3.0,
    PutFn = getPutFn(Enc),
    EvalFn = fun defaultEvalFn/1,

    %% double types support all operations

    Res = allOps({F, {Val}, double, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test boolean operations
%%------------------------------------------------------------

boolOps_test() ->
    {timeout, 60, fun() -> boolOpsTests() end}.
			  
boolOpsTests() ->
    boolOps(msgpack) and boolOps(erlang) and boolOps(mixed).

boolOps(Enc) ->
    io:format("boolOps_test with ~p encoding~n", [Enc]),
    F = <<"f4">>,
    Val = true,
    PutFn = getPutFn(Enc),
    EvalFn = fun defaultEvalFn/1,

    %% boolean types support only equality ops

    Res = eqOpsOnly({F, {Val}, boolean, PutFn, EvalFn, []}) and (anyCompOps({F, {Val}, boolean, PutFn, EvalFn, []}) == false),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test filtering with realistic data
%%------------------------------------------------------------

realisticOps_test() ->
    {timeout, 60, fun() -> realisticOpsTests() end}.

realisticOpsTests() ->
    realisticOps(erlang) and realisticOps(msgpack) and realisticOps(mixed).

realisticOps(Enc) ->
    io:format("realisticOps_test with ~p encoding~n", [Enc]),
    F = <<"laps">>,
    Val = 0,
    PutFn = 
        case Enc of 
            msgpack ->
                fun putKeyRealisticOpsMsgpack/1;
            _ ->
                fun putKeyRealisticOpsErlang/1
        end,
    EvalFn = fun defaultEvalFn/1,
    Res = gteOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test any operations
%%------------------------------------------------------------

anyOps_test() ->
    io:format("anyOps_test~n"),
    F = <<"f5">>,
    Val = em([1,2,3]),
    CompVal = [1,2,3],
    PutFn  = fun putKeyNormalOpsMsgpack/1,
    EvalFn = fun defaultEvalFn/1,
    Res = eqOpsOnly({F, {Val, CompVal}, any, PutFn, EvalFn, []}) and (anyCompOps({F, {Val, CompVal}, any, PutFn, EvalFn, []}) == false),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All normal operations tests
%%------------------------------------------------------------

normalOpsTests() ->
    sint64OpsTests() and varcharOps_test() and boolOps_test() and doubleOps_test() and anyOps_test() and timestampOps_test().

%%=======================================================================
%% Test AND + OR comparators
%%=======================================================================

%%------------------------------------------------------------
%% Test AND operations
%%------------------------------------------------------------

andOps_test() ->
    io:format("andOps_test~n"),
    Cond1 = {'>', {field, <<"f1">>, sint64}, {const, 3}},
    Cond2 = {'=', {field, <<"f3">>, double}, {const, 5.0}},
    Filter = {'and_', Cond1, Cond2},
    PutFn  = fun putKeyNormalOpsMsgpack/1,
    Keys = streamFoldTest(Filter, PutFn, []),
    {N1, NMatch1} = fieldsMatching(Keys, <<"f1">>, 3,   fun(V1,V2) -> V1 > V2 end),
    {N3, NMatch3} = fieldsMatching(Keys, <<"f3">>, 5.0, fun(V1,V2) -> V1 == V2 end),
    ?assertEqual(1, N1),
    ?assertEqual(1, NMatch1),
    ?assertEqual(1, N3),
    ?assertEqual(1, NMatch3),
    pass.

%%------------------------------------------------------------
%% Test OR filtering
%%------------------------------------------------------------

orOps_test() ->
    io:format("orOps_test~n"),
    Cond1 = {'>', {field, <<"f1">>, sint64}, {const, 3}},
    Cond2 = {'=', {field, <<"f3">>, double}, {const, 5.0}},
    Filter = {'or_', Cond1, Cond2},

    PutFn  = fun sut:putKeyNormalOpsMsgpack/1,
    AllKeys = getKeyNormalOps(),

    io:format("All keys = ~p~n", [AllKeys]),

    {NTotal, NExpected1} = fieldsMatching(AllKeys, <<"f1">>, 3,   fun(V1,V2) -> V1 > V2 end),
    {NTotal, NExpected3} = fieldsMatching(AllKeys, <<"f3">>, 5.0, fun(V1,V2) -> V1 == V2 end),

    NExpected =
        case NExpected1 > NExpected3 of
            true ->
                NExpected1;
            _ ->
                NExpected3
        end,

    io:format("NExpected = ~p~n", [NExpected]),

    Keys = streamFoldTest(Filter, PutFn, []),

    io:format("Keys = ~p Size = ~p~n", [Keys, length(Keys)]),

    {N1, NMatch1} = fieldsMatching(Keys, <<"f1">>, 3,   fun(V1,V2) -> V1 > V2 end),
    {N1, NMatch3} = fieldsMatching(Keys, <<"f3">>, 5.0, fun(V1,V2) -> V1 == V2 end),

    io:format("N1 = ~p NMatch1 = ~p NMatch3 = ~p~n", [N1, NMatch1, NMatch3]),

    Res = (N1 == NExpected) and (NMatch1 == NExpected1) and (NMatch3 == NExpected3),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Test all AND + OR ops
%%------------------------------------------------------------

andOrOpsTests() ->
    andOps_test() and orOps_test().

%%=======================================================================
%% Abnormal operations testing
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for abnormal ops
%%-----------------------------------------------------------------------

putKeyAbnormalOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>,   1}, {<<"f2">>, <<"test1">>}, {<<"f3">>,   1.0}, {<<"f4">>,  false}, {<<"f5">>, em([1,2,3])}, {<<"f6">>,  1000}]),
    addKey(Ref, 2, [{<<"f1">>, 2.1}, {<<"f2">>, <<"test2">>}, {<<"f3">>,   2.0}, {<<"f4">>,   true}, {<<"f5">>, em([2,3,4])}, {<<"f6">>, -2000}]),
    addKey(Ref, 3, [{<<"f1">>,   3}, {<<"f2">>,           3}, {<<"f3">>, "3.0"}, {<<"f4">>,  false}, {<<"f5">>, em([3,4,5])}, {<<"f6">>,  3000}]),
    addKey(Ref, 4, [{<<"f1">>,   4}, {<<"f2">>, <<"test4">>}, {<<"f3">>,   4.0}, {<<"f4">>, "true"}, {<<"f5">>,           4}, {<<"f6">>,  4000}]).

%%------------------------------------------------------------
%% Evaluation for abnormal conditions checks that no matching records
%% were returned
%%------------------------------------------------------------

abnormalEvalFn({N,_Nmatch}) ->
    (N == 0).

%%-----------------------------------------------------------------------
%% Test reading data that are not all integers as integers
%%-----------------------------------------------------------------------

badInt_test() ->
    io:format("badInt_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all timestamps as timestamps
%%-----------------------------------------------------------------------

badTimestamp_test() ->
    io:format("badTimestamp_test~n"),
    F = <<"f6">>,
    Val = 2000,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, timestamp, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all binarys as binarys
%%-----------------------------------------------------------------------

badBinary_test() ->
    io:format("badBinary_test~n"),
    F = <<"f2">>,
    Val = <<"test">>,
    PutFn = fun putKeyAbnormalOps/1,
    Res = neqOps({F, {Val}, varchar, PutFn, fun({N,_}) -> N == 4 end, []}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all floats as floats
%%-----------------------------------------------------------------------

badFloat_test() ->
    io:format("badFloat_test~n"),
    F = <<"f3">>,
    Val = 0.0,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, double, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%-----------------------------------------------------------------------
%% Test reading data that are not all booleans as booleans
%%-----------------------------------------------------------------------

badBool_test() ->
    io:format("badBool_test~n"),
    F = <<"f4">>,
    Val = false,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = eqOps({F, {Val}, boolean, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All abnormal ops tests
%%------------------------------------------------------------

abnormalOpsTests() ->
    badInt_test() and badBinary_test() and badFloat_test() and badBool_test() and badTimestamp_test().

%%=======================================================================
%% Test filter on IS NULL and IS NOT NULL
%%
%% Changed to test both supported encodings
%%=======================================================================

isNullFieldOfTypeTestFactory(IsNull, Field) ->
    isNullFieldOfTypeTestFactory(IsNull, Field, msgpack),
    isNullFieldOfTypeTestFactory(IsNull, Field, erlang).

isNullFieldOfTypeTestFactory(IsNull, Field, Enc) ->
    %% if curious why varchar, see comment on isNullOpsFactory
    Type = varchar,
    Val = [],
    CompVal = [],

    PutFn = getPutFn(Enc),

    Keys = getKeyNormalOps(),
    {NTotal, NNull} = fieldsMatching(Keys, Field, [], 
				     fun(V1,[]) -> 
					     case V1 of
						 [] -> true;
						 _ -> false
					     end
				     end),
    
    NNotNull = NTotal - NNull,

    ExpectedRowCount = case IsNull of
        true -> NNull;
        _ -> NNotNull
    end,

    EvalFn = fun({N,_}) -> ?assertEqual(ExpectedRowCount, N) end,
    IsNullOpsFn = case IsNull of
        true -> fun isNullOps/1;
        _ -> fun isNotNullOps/1
    end,
    IsNullOpsFn({Field, {Val, CompVal}, Type, PutFn, EvalFn}).

isNullBinary_test() ->
    isNullFieldOfTypeTestFactory(true, <<"f2">>).

isNullDouble_test() ->
    isNullFieldOfTypeTestFactory(true, <<"f3">>).

isNullBoolean_test() ->
    isNullFieldOfTypeTestFactory(true, <<"f4">>).

isNullInteger_test() ->
    isNullFieldOfTypeTestFactory(true, <<"f6">>).

isNullLargeInteger_test() ->
    isNullFieldOfTypeTestFactory(true, <<"f7">>).

isNotNullBinary_test() ->
    isNullFieldOfTypeTestFactory(false, <<"f2">>).

isNotNullDouble_test() ->
    isNullFieldOfTypeTestFactory(false, <<"f3">>).

isNotNullBoolean_test() ->
    isNullFieldOfTypeTestFactory(false, <<"f4">>).

isNotNullInteger_test() ->
    isNullFieldOfTypeTestFactory(false, <<"f6">>).

isNotNullLargeInteger_test() ->
    isNullFieldOfTypeTestFactory(false, <<"f7">>).

%%=======================================================================
%% Test various exceptional conditions
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for exceptional testing
%%-----------------------------------------------------------------------

putKeyMissingOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, 1}, {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [               {<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3}, {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4}, {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

putKeyFirstMissingOps(Ref) ->
    addKey(Ref, 1, [               {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f1">>, 2}, {<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3}, {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4}, {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

putKeyEmptyOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, 1},  {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f1">>, []}, {<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3},  {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4},  {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

putKeyFirstEmptyOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, []}, {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f1">>,  2}, {<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>,  3}, {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>,  4}, {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

%%------------------------------------------------------------
%% Valid filter, but values are missing for referenced keys
%%------------------------------------------------------------

missingKey_test() ->
    io:format("missingKey_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = gtOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

missingFirstKey_test() ->
    io:format("missingFirstKey_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyFirstMissingOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = gtOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

emptyKey_test() ->
    io:format("emptyKey_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyEmptyOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = gtOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

emptyFirstKey_test() ->
    io:format("emptyFirstKey_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyFirstEmptyOps/1,
    EvalFn = fun defaultEvalFn/1,
    Res = gtOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Filter references a key that doesn't exist -- should return no
%% records
%%------------------------------------------------------------

filterRefMissingKey_test() ->
    io:format("filterRefMissingKey_test~n"),
    F = <<"f0">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Filter specified the wrong type for a valid key
%%------------------------------------------------------------

filterRefWrongType_test() ->
    io:format("filterRefWrongType_test~n"),
    F = <<"f2">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, sint64, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Filter specifies type that's not supported
%%------------------------------------------------------------

filterRefInvalidType_test() ->
    io:format("filterRefInvalidType_test~n"),
    F = <<"f2">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    Res = gtOps({F, {Val}, map, PutFn, EvalFn, []}),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All exceptional tests
%%------------------------------------------------------------

exceptionalTests() ->
    missingKey_test() and emptyKey_test() and filterRefMissingKey_test() and 
        filterRefWrongType_test() and filterRefInvalidType_test().

%%=======================================================================
%% Test scanning
%%=======================================================================

%%-----------------------------------------------------------------------
%% Utilities needed for scanning tests
%%-----------------------------------------------------------------------

streamFoldTestOpts(Opts, FoldFun) ->
    Ref = open(),
    try
        Acc = eleveldb:fold(Ref, FoldFun, [], Opts),
        ok = eleveldb:close(Ref),
        Keys = lists:reverse(Acc)
    catch
        error:Error ->

	    %% This is used for deliberate failure testing, and
	    %% the 'correct' behavior is indicated by return of an
	    %% empty list [].  We fail quietly, so that we don't
	    %% print alarming messages during unit testing
	    %%
	    %%io:format(user, "Caught an error: closing db~n", []),

            ok = eleveldb:close(Ref),
            error(Error)
    end.

%%------------------------------------------------------------
%% Make sure that we iterate over the right number of keys when
%% requesting start and end keys
%%
%% Changed to test scanning for both supported encodings, as well as
%% mixed encoding case
%%------------------------------------------------------------

scanSome_test() ->
    scanSome_test(msgpack) and scanSome_test(erlang) and scanSome_test(mixed).

scanSome_test(Enc) ->
    io:format("scanSome_test with Enc = ~p~n", [Enc]),
    N = 100,
    putKeysObj(N, Enc),
    Opts=[{fold_method, streaming},
          {start_key, <<"key002">>},
          {end_key,  <<"key099">>},
          {end_inclusive,  true}],

    FoldFun = fun({K,_V}, Acc) -> 
                      [K | Acc]
              end,

    Keys = streamFoldTestOpts(Opts, FoldFun),
    Len = length(Keys),
    Res = (Len =:= N-2),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Make sure that we iterate over the right number of keys when
%% iterating over all keys
%%------------------------------------------------------------

scanAll_test() ->
    scanAll_test(msgpack) and scanAll_test(erlang) and scanAll_test(mixed).

scanAll_test(Enc) ->
    io:format("scanAll_test with Enc = ~p~n", [Enc]),
    N = 100,
    putKeysObj(N, Enc),
    Opts=[{fold_method, streaming}],
    FoldFun = fun({K,V}, Acc) -> 
                      [getKeyVal(K,V) | Acc]
              end,
    Keys = streamFoldTestOpts(Opts, FoldFun),
    Len = length(Keys),
    Res = (Len =:= N),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% Don't include the first key
%%------------------------------------------------------------

scanNoStart_test() ->
    scanNoStart_test(msgpack) and scanNoStart_test(erlang) and scanNoStart_test(mixed).

scanNoStart_test(Enc) ->
    io:format("scanNoStart_test with Enc = ~p~n", [Enc]),
    N = 100,
    putKeysObj(N, Enc),
    Opts=[{fold_method, streaming},
          {start_inclusive, false},
          {start_key, <<"key001">>}],

    FoldFun = fun({K,_V}, Acc) -> 
                      [K | Acc]
              end,

    Keys = streamFoldTestOpts(Opts, FoldFun),
    [Key1 | _Rest] = Keys,
    Len = length(Keys),
    Res = ((Len =:= N-1) and (Key1 =:= <<"key002">>)),
    ?assert(Res),
    Res.

scanNoStartTest() ->
    N = 100,
    putKeysObj(N),
    Opts=[{fold_method, streaming},
          {start_inclusive, false},
          {start_key, <<"key001">>}],

    FoldFun = fun({K,_V}, Acc) -> 
                      [K | Acc]
              end,

    streamFoldTestOpts(Opts, FoldFun).

%%------------------------------------------------------------
%% Don't include the start or end key
%%------------------------------------------------------------

scanNoStartOrEnd_test() ->
    scanNoStartOrEnd_test(msgpack) and scanNoStartOrEnd_test(erlang) and scanNoStartOrEnd_test(mixed).

scanNoStartOrEnd_test(Enc) ->
    io:format("scanNoStartOrEnd_test with Enc = ~p~n", [Enc]),
    N = 100,
    putKeysObj(N, Enc),
    Opts=[{fold_method, streaming},
          {start_inclusive, false},
          {end_inclusive, false},
          {start_key, <<"key001">>},
          {end_key,   <<"key100">>}],

    FoldFun = fun({K,_V}, Acc) -> 
                      [K | Acc]
              end,

    Keys = streamFoldTestOpts(Opts, FoldFun),

    [KeyFirst | _Rest] = Keys,
    KeyLast = lists:last(Keys),
    
    Len = length(Keys),
    Res = ((Len =:= N-2) and (KeyFirst =:= <<"key002">>) and (KeyLast =:= <<"key099">>)),
    ?assert(Res),
    Res.

%%------------------------------------------------------------
%% All scanning tests
%%------------------------------------------------------------

scanTests() ->
    scanAll_test() and scanSome_test() and scanNoStart_test() and scanNoStartOrEnd_test().

%%------------------------------------------------------------
%% All tests in this file
%%------------------------------------------------------------

allTests() ->
    packObj_test() and normalOpsTests() and abnormalOpsTests() and exceptionalTests() 
        and scanTests().

%%=======================================================================
%% Test code to filter & decode TS keys from a leveldb table
%%=======================================================================

readKeysFromTable(Table) ->    

    Cond1 = {'=', {field, <<"user">>, varchar},   {const, <<"user_1">>}},
    Cond2 = {'<', {field, <<"time">>, timestamp}, {const, 11000000}},
    Cond3 = {'>', {field, <<"time">>, timestamp}, {const,  9990000}},
    Filter = {'and_', Cond1, {'and_', Cond2, Cond3}},
    Opts=[{fold_method, streaming},
          {range_filter, Filter},
          {encoding, msgpack}],
    io:format("Filter = ~p~n", [lists:flatten([Filter])]),
    TableName = "/Users/eml/projects/riak/riak_end_to_end_timeseries/dev/dev1/data/leveldb/" ++ Table,
    io:format("Attemping to open ~ts~n", [TableName]),
    Ref = open(TableName),
    Bucket = {<<"GeoCheckin">>, <<"GeoCheckin">>},
    FoldFun = fun({_K,V}, _Acc) -> 
                      Contents = getKeyVal(Bucket, <<"key">>, V),
                      io:format("Val = ~p~n", [lists:flatten(Contents)])
              end,

    eleveldb:fold(Ref, FoldFun, [], Opts),
    ok = eleveldb:close(Ref).

r() ->
    readKeysFromTable("1096126227998177188652763624537212264741949407232").
