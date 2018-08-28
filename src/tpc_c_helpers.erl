-module(tpc_c_helpers).

-export([uniform_random_int/2,
         uniform_random_int/3,
         non_uniform_random_int/4,
         uniform_random_float/3,
         random_a_string/2,
         random_a_string/1,
         random_numeric_string/1,
         random_data/0,
         generate_customer_last_name/1]).

-define(ALPHABETIC_CHARS, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
-define(NUMERIC_CHARS, "0123456789").

uniform_random_int(Start, End) ->
    random:uniform(End - Start + 1) + Start - 1.

uniform_random_int(Start, End, Filter) ->
    Random = random:uniform(End - Start + 1) + Start - 1,
    case Random of
        Filter -> uniform_random_int(Start, End, Filter);
        _ -> Random
    end.

non_uniform_random_int(C, A, X, Y) ->
    (((uniform_random_int(0, A) bor uniform_random_int(X, Y)) + C) rem (Y - X + 1)) + X.

uniform_random_float(Min, Max, Precision) ->
    Pow = math:pow(10, Precision),
    AMin = Pow * Min,
    AMax = Pow * Max,
    Random = random:uniform() * (AMax - AMin) + AMin,
    Random / Pow.

random_a_string(Min, Max) ->
    Length = uniform_random_int(Min, Max),
    random_string(?ALPHABETIC_CHARS ++ ?NUMERIC_CHARS, Length).

random_a_string(Length) ->
    random_string(?ALPHABETIC_CHARS ++ ?NUMERIC_CHARS, Length).

random_numeric_string(Length) ->
    random_string(?NUMERIC_CHARS, Length).

random_string(Chars, Length) ->
    CharsTuple = list_to_tuple(Chars),
    CharsSize = size(CharsTuple),
    FoldFun = fun(_, Acc) -> [element(random:uniform(CharsSize), CharsTuple) | Acc] end,
    lists:foldl(FoldFun, "", lists:seq(1, Length)).

random_data() ->
    Alea = random_a_string(26, 50),
    case uniform_random_int(1, 10) of
        1 ->
            AleaLength = length(Alea),
            Random = uniform_random_int(0, AleaLength - 8),
            lists:sublist(Alea, 1, Random + 1) ++ "ORIGINAL" ++ lists:sublist(Alea, Random + 9, AleaLength);
        _ ->
            Alea
    end.

generate_customer_last_name(Random) ->
    Syllables = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"],
    lists:foldl(fun(I, Acc) ->
                    lists:nth(nth_digit(I, 3, Random) + 1, Syllables) ++ Acc
                end, "", lists:seq(1, 3)).

nth_digit(Nth, NDigits, Number) ->
    case NDigits - Nth of
        0 ->
            Number rem 10;
        _ ->
            nth_digit(Nth, NDigits - 1, trunc(Number / 10))
    end.
