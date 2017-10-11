DROP TYPE IF EXISTS search_parser_state CASCADE;

CREATE TYPE search_parser_state AS (
    search_query text,
    parentheses_stack int,
    empty_parentheses boolean,
    skip_for int,
    current_token text,
    current_index int,
    current_char text,
    tokens text[]
);

CREATE OR REPLACE FUNCTION ts_append_current_token(state search_parser_state)
RETURNS search_parser_state AS $$
BEGIN
    IF state.current_token != '' THEN
        state.tokens := array_append(state.tokens, state.current_token);
        state.current_token := '';
    END IF;
    RETURN state;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ts_tokenize_character(state search_parser_state)
RETURNS search_parser_state AS $$
BEGIN
    IF state.current_char = '(' THEN
        state.tokens := array_append(state.tokens, '(');
        state.parentheses_stack := state.parentheses_stack + 1;
        state.empty_parentheses := true;
    ELSIF state.current_char = ')' THEN
        IF (state.current_token != '') THEN
            state.empty_parentheses := false;
        END IF;
        IF (state.parentheses_stack > 0 AND NOT state.empty_parentheses) THEN
            state := ts_append_current_token(state);
            state.tokens := array_append(state.tokens, ')');
            state.parentheses_stack := state.parentheses_stack - 1;
        END IF;
    ELSIF state.current_char = '"' THEN
        state.skip_for := position('"' IN substring(
            state.search_query FROM state.current_index + 1
        ));
        IF state.skip_for > 0 THEN
            state.tokens = array_append(
                state.tokens,
                substring(
                    state.search_query
                    FROM state.current_index FOR state.skip_for + 1
                )
            );
        ELSE
            state.current_token = state.current_token || state.current_char;
        END IF;
    ELSIF (
        state.current_char = '-' AND
        (
            state.current_index = 1 OR
            substring(
                state.search_query FROM state.current_index - 1 FOR 1
            ) = ' '
        )
    ) THEN
        state.tokens = array_append(state.tokens, '-');
    ELSIF state.current_char = ' ' THEN
        state := ts_append_current_token(state);
        IF substring(
            state.search_query FROM state.current_index FOR 4
        ) = ' or ' THEN
            state.skip_for := 2;

            -- remove duplicate OR tokens
            IF state.tokens[array_length(state.tokens, 1)] != ' | ' THEN
                state.tokens := array_append(state.tokens, ' | ');
            END IF;
        END IF;
    ELSE
        state.current_token = state.current_token || state.current_char;
    END IF;
    RETURN state;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ts_tokenize(search_query text) RETURNS text[] AS $$
DECLARE
    state search_parser_state;
BEGIN
    SELECT
        search_query::text AS search_query,
        0::int AS parentheses_stack,
        true as empty_parentheses,
        0 AS skip_for,
        ''::text AS current_token,
        0 AS current_index,
        ''::text AS current_char,
        '{}'::text[] AS tokens
    INTO state;

    state.search_query := lower(trim(
        regexp_replace(search_query, '"+', '"', 'g')
    ));

    FOR state.current_index IN (
        SELECT generate_series(1, length(state.search_query))
    ) LOOP
        state.current_char := substring(
            search_query FROM state.current_index FOR 1
        );

        IF state.skip_for > 0 THEN
            state.skip_for := state.skip_for - 1;
            CONTINUE;
        END IF;

        state := ts_tokenize_character(state);
    END LOOP;
    state := ts_append_current_token(state);

    state.tokens := array_nremove(state.tokens, '(', -state.parentheses_stack);

    RETURN state.tokens;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ts_process_tokens(tokens text[]) RETURNS text AS $$
DECLARE
    result_query text;
    previous_value text;
    value text;
BEGIN
    result_query := '';
    FOREACH value IN ARRAY tokens LOOP
        IF left(value, 1) = '"' AND right(value, 1) = '"' THEN
            value := phraseto_tsquery('pg_catalog.simple', value);
        ELSIF value NOT IN ('(', ' | ', ')', '-') THEN
            value := quote_literal(value) || ':*';
        END IF;

        IF previous_value = '-' THEN
            IF value = '(' THEN
                value := '!' || value;
            ELSE
                value := '!(' || value || ')';
            END IF;
        END IF;

        IF value != '-' THEN
            SELECT
                CASE
                    WHEN result_query = '' THEN value
                    WHEN (
                        previous_value IN ('!(', '(', ' | ') OR
                        value IN (')', ' | ')
                    ) THEN result_query || value
                    ELSE result_query || ' & ' || value
                END
            INTO result_query;
        END IF;
        previous_value := value;
    END LOOP;

    RETURN result_query;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ts_parse(search_query text) RETURNS text AS $$
    SELECT ts_process_tokens(ts_tokenize(search_query));
$$ LANGUAGE SQL;


-- remove first N elements equal to the given value from the array (array
-- must be one-dimensional)
--
-- If negative value is given as the third argument the removal of elements
-- starts from the last array element.
CREATE OR REPLACE FUNCTION array_nremove(anyarray, anyelement, int)
RETURNS ANYARRAY AS $$
    WITH replaced_positions AS (
        SELECT UNNEST(
            CASE
            WHEN $2 IS NULL THEN
                '{}'::int[]
            WHEN $3 > 0 THEN
                (array_positions($1, $2))[1:$3]
            WHEN $3 < 0 THEN
                (array_positions($1, $2))[
                    (cardinality(array_positions($1, $2)) + $3 + 1):
                ]
            ELSE
                '{}'::int[]
            END
        ) AS position
    )
    SELECT COALESCE((
        SELECT array_agg(value)
        FROM unnest($1) WITH ORDINALITY AS t(value, index)
        WHERE index NOT IN (SELECT position FROM replaced_positions)
    ), $1[1:0]);
$$ LANGUAGE SQL IMMUTABLE;
