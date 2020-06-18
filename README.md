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
conda create --name cloudmask --file cloudmaskenv.lock 
conda activate cloudmask
PIP_USER=True pip install git+https://github.com/jonathan-taylor/regreg.git
```

You can then verify that the regreg package was correctly installed via:
```
python -c "import regreg"
```

You may need to re-run the conda commands upon hub restart:

```
conda create --name cloudmask --file cloudmaskenv.lock 
conda activate cloudmask
```

but the pip one should leave regreg installed across reboots.