import setuptools

setuptools.setup(
    name="mobility-tools",
    version="0.0.1",
    author="Antoine Gauchot, Anne-Sophie Girot, Louise Gontier, Félix Pouchain",
    author_email="antoine.gauchot@arep.fr, a.girot@elioth.fr, l.gontier@elioth.fr, felix.pouchain@arep.fr",
    description="A tool to simulate the mobility behaviours of the inhabitants of a given region.",
    url="https://github.com/mobility-team/mobility",
    packages=setuptools.find_packages(),
    python_requires=">=3.9",
    install_requires=["numpy", "pandas", "requests", "pyarrow", "openpyxl"],
)
