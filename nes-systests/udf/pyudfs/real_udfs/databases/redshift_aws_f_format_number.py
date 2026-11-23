# codon-fuzz: entry=f_format_number args=FLOAT64,VARSIZED,VARSIZED,INT64,INT64,BOOLEAN returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1234567.891, ",", ".", 3, 2, false], [-98765.4321, ".", ",", 3, 3, false], [42.5, ",", ".", 3, 2, true], [1234567.0, " ", ",", 3, 0, false], [123456789.125, "'", ".", 4, 1, true], [0.07, ",", ".", 3, 2, false], [1999.996, ",", ".", 3, 2, false]]
# real-udf: origin=databases id=redshift_aws_f_format_number requires= wrapped=0 ported=1
# Ported: `except X, e:` replaced with `except X as e:` (Python 3.14 reads the old form as `except (X, e):`).
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_format_number%28float%2Cvarchar%2Cvarchar%2Cint%2Cint%2Cbool%29/function.sql#L10
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_format_number(value, group_sep, decimal_sep, group_length, prec, sign):
    from collections import deque
    from math import modf

    def int_grouper_as_string(int_value, group_sep, group_length):
        d = deque(str(int(int_value)))
        grouped = []
        c = 0
        while True:
            try:
                x = d.pop()
            except IndexError:
                break
            if c and not c % group_length:
                grouped.append(group_sep)
            c += 1
            grouped.append(x)
        grouped.reverse()
        return ''.join(grouped)


    def get_decimal_as_string(fract_value, precision):
        if precision > 0:
            prec_string = '{{0:.{precision}f}}'.format(precision=precision)
            fract_string = prec_string.format(fract_value)
            return fract_string.split('.')[-1]
        else:
            return ''


    def f_format_number(value, group_sep=',', decimal_sep='.', group_length=3,
                        precision=2, sign=False):
        if value is None:
            return None
        try:
            value_float = float(value)
        except ValueError as e:
            print('A problem occurred with formatting, numeric value was expected.')
            raise(e)
        try:
            assert decimal_sep != group_sep
        except AssertionError as e:
            print('A problem occurred with formatting, group and decimal separators should not be equal!')
            raise(e)

        if value < 0:
            sign_symbol = '-'
        elif sign:
            sign_symbol = '+'
        else:
            sign_symbol = ''

        fract_part, int_part = modf(abs(value_float))
        int_group = int_grouper_as_string(int_part, group_sep, group_length)
        dec = get_decimal_as_string(fract_part, prec)

        res = sign_symbol+int_group

        if dec != '':
            res += decimal_sep+dec

        return res


    return f_format_number(value, group_sep, decimal_sep, group_length,
    	   prec, sign)
