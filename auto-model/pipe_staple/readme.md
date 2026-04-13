# The Automotive Visual Inspection Dataset (AutoVI): A Genuine Industrial Production Dataset for Unsupervised Anomaly Detection
Modern industrial production lines must be set up with robust defect inspection modules that are able to withstand high product variability. This means that in a context of industrial production, new defects that are not yet known may appear, and must therefore be identified.

On industrial production lines, the typology of potential defects is vast (texture, part failure, logical defects, etc.). Inspection systems must therefore be able to detect non-listed defects, i.e. not-yet-observed defects upon the development of the inspection system. To solve this problem, research and development of unsupervised AI algorithms on real-world data is required.

Renault Group and the Université de technologie de Compiègne (Roberval and Heudiasyc Laboratories) have jointly developed the _Automotive Visual Inspection Dataset (AutoVI)_, the purpose of which is to be used as a scientific benchmark to compare and develop advanced unsupervised anomaly detection algorithms under real production conditions. The images were acquired on Renault Group's automotive production lines, in a genuine industrial production line environment, with variations in brightness and lighting on constantly moving components. This dataset is representative of actual data acquisition conditions on automotive production lines.

The evaluation code can be found at <https://github.com/phcarval/autovi_evaluation_code>.

# Disclaimer
All defects shown were intentionally created on Renault Group's production lines for the purpose of producing this dataset. The images were examined and labeled by Renault Group experts, and all defects were corrected after shooting.

# Defect description
The black pipe should be held by the metallic staple on the upper left of the brake wheel.

![[defect_example.png]]
On the right, correct pipe placement. On the left, pipe is not held by the staple.

# License
Copyright © 2023-2024 Renault Group

This work is licensed under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. To view a copy of the license, visit <http://creativecommons.org/licenses/by-nc-sa/4.0/>.

For using the data in a way that falls under the commercial use clause of the license, please contact us.

# Attribution
Please use the following for citing the dataset in scientific work:
Carvalho, P., Lafou, M., Durupt, A., Leblanc, A, & Grandvalet, Y. (2024). The Automotive Visual Inspection Dataset (AutoVI): A Genuine Industrial Production Dataset for Unsupervised Anomaly Detection \[Dataset\]. https://doi.org/10.5281/zenodo.10459003

# Contact
If you have any questions or remarks about this dataset, please contact us at philippe.carvalho@utc.fr, meriem.lafou@renault.com, alexandre.durupt@utc.fr, antoine.leblanc@renault.com, yves.grandvalet@utc.fr.
