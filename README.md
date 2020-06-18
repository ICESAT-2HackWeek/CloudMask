# CloudMask
Fetch, classify, and label ICESat-2 data


# Collaborators
- Shane Grigsby (lead)
- Facu Sapienza
- [Alice Cima](https://github/alicecima)
- Fernando Pérez
- Matt Siegfried
- Tasha Snow


# Setup

**This is preliminary and only for our pangeo hub for now**

To use the tools in this project, use:

```
conda install --yes -c conda-forge cython gcc_linux-64
python -m pip install --user --upgrade git+https://github.com/jonathan-taylor/regreg.git
```

You can then verify that the regreg package was correctly installed via:

```
python -c "import regreg"
```

The above commands only need to be re-run if an upgrade to `regreg` is needed. Otherwise the user-level installation of `regreg` will survive across hub reboots.
