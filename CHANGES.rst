Changelog
=========


5.3 (unreleased)
----------------

- PCM-1818 migrate/map multiple (collection) views [fiterbek]
- PCM-1591 enabled diff support for SubsiteFolder in portal_diff [ajung]
- PCM-1811 improper handling of recurse=False in path criteria 
  conversion [ajung]
- PCM-1576 sortable_view layout -> tabular_view layout [ajung]
- PCM-1822 migrated PFG DateField to DateTime (rather than Date) [ajung]
- PCM-1778 set local roles for SubsiteFolder based Folder + default page [ajung]
- PCM-1818 adjusted layout migration [ajung]
- PCM-1864 fix for migrating folder review_state "private" [ajung]
- PCM-1847 whitelist ugent.collectiondelete.interfaces.ICollectionDelete [ajung]
- PCM-1813 always migrate type constraints (independent of navigation root) [ajung]
- PCM-1818 switching to listing_view only for Topics [ajung]
- PCM-1868 portal type replacements for old type criterion [ajung]
- PCM-1864 fix for review states [ajung]
- PCM-1878 ignore phddefense_view [ajung]
- PCM-1893 ignore manualgroup_view [ajung]
- PCM-1899 respect __ac_local_roles_block__ [ajung]
- migrate PhdDefense.streamingUrl properly [ajung]
- PCM-1901 fix for mailer action garbage (PFG) [ajung]
- PCM-1864 simplified rewrite of set_review_workflow() [ajung]


5.2 (2020-08-10)
----------------

- PCM-1773, PCM-1767, PCM-1760 [ajung]
- Fix (folder) ordering by Andreas (PCM-1774) [fiterbek] 
- Patches for PCM-1780 (endless recursion in outputfilters) [ajung]
- support for multiple mailer actions (PCM-1726) [ajung]
- Event related fixes (PCM-1782) [ajung]
- fixed issue with creation of the root folder (PCM-1780) [ajung]


5.1 (2020-07-17)
----------------

- A bunch of changes by Andreas
- Support theme selection during migration [slipeete]


5.0 (2020-07-06)
----------------
- First Plone 5 release (PTS-779) [fiterbek]

- Initial release.
  [ajung]
