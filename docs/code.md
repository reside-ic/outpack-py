# Code examples

Giving a short explanation here for function:

(add_function)=
## Add Function

```python
def add(x, y):
    return x + y
```

This is an example of running code in the docs. You can define everything in the code block like so:


```{runblock} pycon
>>> def add(x, y):
...     return x + y

>>> print(add(1, 2))
```

This is a potential pattern I came up with for code that requires setup

```{runblock} pycon
>>> exec(open("./docs/setup/helper.py").read()) # ignore
>>> print(multiply(1, 2))
```

```{warning}
Easy there buckaroo
```

```{error}
No can do
```

```{note}
For your information
```