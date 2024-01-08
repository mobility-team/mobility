import setuptools

setuptools.setup(
    name="mobility-tools",
    version="0.1",
    author="Louise Gontier, Félix Pouchain, Capucine-Marin Dubroca-Voisin",
    author_email="l.gontier@elioth.fr, felix.pouchain@arep.fr, capucine-marin.dubroca-voisin@arep.fr",
    description="A tool to simulate the mobility behaviours of the inhabitants of a given region.",
    url="https://github.com/mobility-team/mobility",
    packages=setuptools.find_packages(),
    python_requires=">=3.9",
    install_requires=["numpy", "pandas", "requests", "pyarrow", "openpyxl"],
)
